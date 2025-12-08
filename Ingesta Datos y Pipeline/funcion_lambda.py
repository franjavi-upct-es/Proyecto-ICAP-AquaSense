"""
Función Lambda para AquaSenseCloud
Pipeline de procesamiento de datos del Mar Menor

Descripción:
    Esta función Lambda se ejecuta automáticamente cuando se sube un archivo CSV
    al bucket S3 configurado. Procesa TODOS los archivos CSV del bucket,
    detecta fechas duplicadas y sobrescribe con el último valor encontrado.

Funcionalidades:
    - Lectura de TODOS los archivos CSV del bucket (no solo el trigger)
    - Detección y sobrescritura de fechas duplicadas (última gana)
    - Parsing flexible de múltiples formatos de fecha
    - Cálculo de métricas mensuales (temperatura media, desviación máxima, diferencias)
    - Detección de alarmas (desviación > 0.5ºC)
    - Actualización de DynamoDB con datos procesados
    - Envió de notificaciones SNS
    - Ajuste de mes: si el día es <= 3, se asigna al mes anterior

Triggers:
    - S3 ObjectCreated:* en bucket proy-marmenor-data-raw-*
    - Filtro: archivos *.csv

Variables de Entorno Requeridas:
    - DYNAMODB_TABLE: Nombre de la tabla DynamoDB
    - SNS_TOPIC_ARN: ARN del topic SNS para alarmas
    - DESVIATION_THRESHOLD: Umbral de desviación (default: 0.5)
"""

import json
import boto3
import csv
import os
import urllib.parse
from datetime import datetime, timedelta
from decimal import Decimal
from collections import defaultdict

# ============================================================================
# CONFIGURACIÓN Y CLIENTES AWS
# ============================================================================

s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
sns_client = boto3.client("sns")

# Variables de entorno
DYNAMODB_TABLE = os.environ["DYNAMODB_TABLE"]
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
DEVIATION_THRESHOLD = Decimal(str(os.environ.get("DEVIATION_THRESHOLD", "0.5")))

# Tabla DynamoDB
table = dynamodb.Table(DYNAMODB_TABLE)


# ============================================================================
# FUNCIONES AUXILIARES
# ============================================================================

def round_decimal(value):
    """Redondea un valor Decimal a 4 decimales."""
    return value.quantize(Decimal("0.0001"))

def adjust_month_for_date(fecha_dt):
    """
    Ajusta el mes de una fecha según el día.
    Si el día es <= 3, se asigna al mes anterior.
    
    Args:
        fecha_dt: datetime object
    
    Returns:
        str: Mes en formato 'YYYY-MM' ajustado
    """
    if fecha_dt.day <= 3:
        # Mover al mes anterior
        adjusted_date = fecha_dt.replace(day=1) - timedelta(days=1)
        return adjusted_date.strftime('%Y-%m')
    else:
        return fecha_dt.strftime('%Y-%m')

def send_alert(fecha_str, desviacion, temp_media, filename):
    """Envía una alerta por SNS."""
    try:
        fecha_dt = datetime.strptime(fecha_str, '%Y/%m/%d')
        fecha_formatted = fecha_dt.strftime("%Y-%m-%d")
        
        subject = "⚠️ Alarma Mar Menor - Desviación Alta Detectada"
        message = f"""
⚠️ ALERTA DE TEMPERATURA - MAR MENOR
=========================================
Se ha detectado una desviación superior al umbral.

📊 DETALLES
-----------------------------------------
- Fecha:        {fecha_formatted}
- Desviación:   {float(desviacion):.4f}°C ⚠️
- Umbral:       {float(DEVIATION_THRESHOLD):.2f}°C
- Temp Media:   {float(temp_media):.2f}°C
- Archivo:      {filename}

--
Sistema AquaSenseCloud
        """.strip()

        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN, 
            Subject=subject, 
            Message=message
        )
    except Exception as e:
        print(f"Error enviando alerta SNS: {str(e)}")


def get_all_csv_files(bucket):
    """Obtiene lista de todos los archivos CSV del bucket ordenados alfabéticamente."""
    try:
        response = s3_client.list_objects_v2(Bucket=bucket)
        
        if 'Contents' not in response:
            return []
        
        # Filtrar solo CSVs y ordenar alfabéticamente
        csv_files = sorted([
            obj['Key'] for obj in response['Contents'] 
            if obj['Key'].lower().endswith('.csv')
        ])
        
        return csv_files
        
    except Exception as e:
        print(f"Error listing bucket contents: {e}")
        return []


def process_csv_file(bucket, key, local_dir):
    """
    Descarga y procesa un archivo CSV.
    Retorna diccionario de fechas: {'2023-01-15': {'temp': 20, 'sd': 0.5, 'source': 'file.csv', 'adjusted_month': '2023-01'}}
    """
    local_filename = os.path.join(local_dir, os.path.basename(key))
    daily_data = {}
    
    try:
        s3_client.download_file(bucket, key, local_filename)
        
        with open(local_filename, encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=',')
            
            for row in reader:
                # Parseo de fecha
                try:
                    fecha = datetime.strptime(row['Fecha'], '%Y/%m/%d')
                except ValueError:
                    try:
                        fecha = datetime.strptime(row['Fecha'], '%Y-%m-%d')
                    except ValueError:
                        print(f"Warning: Invalid date format in {key}: {row['Fecha']}")
                        continue
                
                fecha_str = fecha.strftime('%Y-%m-%d')
                temp_media = round_decimal(Decimal(row['Medias']))
                desviacion = round_decimal(Decimal(row['Desviaciones']))
                
                # Ajustar mes según el día (si día <= 3, va al mes anterior)
                adjusted_month = adjust_month_for_date(fecha)

                # Guardar (sobrescribe si ya existe en este archivo)
                daily_data[fecha_str] = {
                    'temp': temp_media,
                    'sd': desviacion,
                    'source': key,
                    'adjusted_month': adjusted_month  # Mes ajustado
                }
        
        return daily_data
        
    except Exception as e:
        print(f"Error processing file {key}: {e}")
        return {}
    
    finally:
        if os.path.exists(local_filename):
            os.remove(local_filename)


# ============================================================================
# HANDLER PRINCIPAL
# ============================================================================

def lambda_handler(event, context):

    bucket = event['Records'][0]['s3']['bucket']['name']
    trigger_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
    
    local_dir = "/tmp"
    
    try:
        # ============================================================
        # PASO 1: Obtener TODOS los archivos CSV del bucket
        # ============================================================
        all_csv_files = get_all_csv_files(bucket)
        
        if not all_csv_files:
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "message": "No CSV files found in bucket",
                    "trigger_file": trigger_key
                })
            }
        
        # ============================================================
        # PASO 2: Procesar todos los archivos y fusionar datos
        # ============================================================
        merged_daily_data = {}  # {'2023-01-15': {'temp': 20, 'sd': 0.5, 'source': 'file.csv', 'adjusted_month': '2023-01'}}
        alerts_sent = 0
        total_rows = 0
        files_processed = 0
        month_adjustments = 0  # Contador de fechas ajustadas
        
        for csv_key in all_csv_files:
            file_data = process_csv_file(bucket, csv_key, local_dir)
            
            if file_data:
                files_processed += 1
                total_rows += len(file_data)
                
                # Fusionar: última aparición sobrescribe
                for fecha, data in file_data.items():

                    # Verificar si se ajustó el mes
                    fecha_dt = datetime.strptime(fecha, '%Y-%m-%d')
                    original_month = fecha_dt.strftime('%Y-%m')
                    if data['adjusted_month'] != original_month:
                        month_adjustments += 1
                    
                    merged_daily_data[fecha] = data
                    
                    # Detectar alertas
                    if data['sd'] > DEVIATION_THRESHOLD:
                        send_alert(fecha.replace('-', '/'), data['sd'], data['temp'], csv_key)
                        alerts_sent += 1
        
        duplicates_found = total_rows - len(merged_daily_data)
        
        # ============================================================
        # PASO 3: Agrupar por mes AJUSTADO y calcular métricas
        # ============================================================
        monthly_data = defaultdict(dict)  # {'2023-01': {'2023-01-05': {...}, ...}}
        
        for fecha_str, data in merged_daily_data.items():
            # Usar el mes ajustado en lugar del mes natural
            mes = data['adjusted_month']
            monthly_data[mes][fecha_str] = data
        
        # ============================================================
        # PASO 4: Actualizar DynamoDB
        # ============================================================
        updated_months = 0
        
        for mes, dates_dict in sorted(monthly_data.items()):
            try:
                # Calcular métricas del mes
                all_temps = [d['temp'] for d in dates_dict.values()]
                all_sds = [d['sd'] for d in dates_dict.values()]
                
                max_temp = max(all_temps)
                max_sd = max(all_sds)
                mean_temp = round_decimal(sum(all_temps) / len(all_temps))
                count = len(all_temps)

                # Calcular diferencia con mes anterior
                current_month_dt = datetime.strptime(mes, '%Y-%m')
                previous_month_dt = current_month_dt.replace(day=1) - timedelta(days=1)
                previous_month = previous_month_dt.strftime('%Y-%m')

                # Buscar mes anterior en datos procesados o DB
                if previous_month in monthly_data:
                    prev_temps = [d['temp'] for d in monthly_data[previous_month].values()]
                    prev_max = max(prev_temps)
                else:
                    # Buscar en DynamoDB
                    previous_item_resp = table.get_item(Key={'monthYear': previous_month})
                    previous_item = previous_item_resp.get('Item', {})
                    prev_max = Decimal(str(previous_item.get('max_temp', 0)))

                max_diff_temp = round_decimal(max_temp - prev_max)

                # Guardar en DynamoDB
                table.put_item(
                    Item={
                        'monthYear': mes,
                        'max_temp': max_temp,
                        'max_sd': max_sd,
                        'mean_temp': mean_temp,
                        'max_diff_temp': max_diff_temp,
                        'mean_temp_count': count,
                        'last_updated': datetime.now().isoformat()
                    }
                )

                updated_months += 1

            except Exception as e:
                print(f"Error updating month {mes}: {e}")
                raise
        
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Processing completed successfully",
                "trigger_file": trigger_key,
                "files_processed": files_processed,
                "total_rows": total_rows,
                "unique_dates": len(merged_daily_data),
                "duplicates_overwritten": duplicates_found,
                "month_adjustments": month_adjustments,
                "months_updated": updated_months,
                "alerts_sent": alerts_sent
            })
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise Exception(f"Error processing bucket {bucket}: {str(e)}")