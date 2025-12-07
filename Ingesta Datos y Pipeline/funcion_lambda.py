"""
Función Lambda para AquaSenseCloud
Pipeline de procesamiento de datos del Mar Menor

Descripción:
    Esta función Lambda se ejecuta automáticamente cuando se sube un archivo CSV
    al bucket S3 configurado. Procesa los datos de temperatura del Mar Menor,
    calcula métricas mensuales y envía alarmas cuando es necesario.

Funcionalidades:
    - Lectura de archivos CSV desde S3
    - Parsing flexible de múltiples formatos de fecha
    - Cálculo de métricas mensuales (temperatura media, desviación máxima, diferencias)
    - Detección de alarmas (desviación > 0.5ºC)
    - Actualización de DynamoDB con datos procesados
    - Envió de notificaciones SNS

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
    """Redondea un valor Decimal a 2 decimales."""
    return value.quantize(Decimal("0.01"))

def send_alert(fecha_str, desviacion, temp_media, filename):
    """Envía una alerta por SNS (Corregido error de sintaxis)."""
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


# ============================================================================
# HANDLER PRINCIPAL
# ============================================================================

def lambda_handler(event, context):
    print("Event received: " + json.dumps(event, indent=2))

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
    local_filename = f"/tmp/{os.path.basename(key)}"
    
    try:
        # Descargar
        s3_client.download_file(bucket, key, local_filename)

        # Procesar CSV
        with open(local_filename, encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=',')
            data = {}
            total_rows = 0
            alerts_sent = 0

            for row in reader:
                total_rows += 1
                
                # Parseo de fecha
                try:
                    fecha = datetime.strptime(row['Fecha'], '%Y/%m/%d')
                except ValueError:
                    fecha = datetime.strptime(row['Fecha'], '%Y-%m-%d')
                
                mes = fecha.strftime('%Y-%m') # Clave PK
                
                temp_media = round_decimal(Decimal(row['Medias']))
                desviacion = round_decimal(Decimal(row['Desviaciones']))

                # Agregación en memoria
                if mes not in data:
                    data[mes] = {
                        'max_temp': temp_media,
                        'max_sd': desviacion,
                        'mean_temp_sum': temp_media,
                        'mean_temp_count': 1
                    }
                else:
                    data[mes]['max_temp'] = max(data[mes]['max_temp'], temp_media)
                    data[mes]['max_sd'] = max(data[mes]['max_sd'], desviacion)
                    data[mes]['mean_temp_sum'] += temp_media
                    data[mes]['mean_temp_count'] += 1

                # Alertas
                if desviacion > DEVIATION_THRESHOLD:
                    send_alert(fecha.strftime('%Y/%m/%d'), desviacion, temp_media, key)
                    alerts_sent += 1

            # Actualizar DynamoDB (SOBREESCRITURA)
            updated_months = 0
            
            for mes, metrics in data.items():
                try:
                    # Cálculo directo de la media del mes actual (sin datos históricos)
                    current_mean_temp = round_decimal(metrics['mean_temp_sum'] / metrics['mean_temp_count'])

                    current_month_dt = datetime.strptime(mes, '%Y-%m')
                    previous_month_dt = current_month_dt.replace(day=1) - timedelta(days=1)
                    previous_month = previous_month_dt.strftime('%Y-%m')

                    # 1. Buscar mes anterior en DB
                    previous_item_resp = table.get_item(Key={'monthYear': previous_month})
                    previous_item = previous_item_resp.get('Item', {})
                    prev_max_db = Decimal(previous_item.get('max_temp', 0))

                    # 2. Buscar mes anterior en el archivo actual
                    if previous_month in data:
                        prev_max_file = data[previous_month]['max_temp']
                    else:
                        prev_max_file = Decimal(0)
                    
                    # El máximo real del mes anterior es el mayor entre DB y File
                    previous_max_final = max(prev_max_db, prev_max_file)

                    # Diferencia con el mes anterior
                    max_diff_temp = round_decimal(metrics['max_temp'] - previous_max_final)

                    # Guardar item (SOBRESCRIBE la fila existente para este mes)
                    table.put_item(
                        Item={
                            'monthYear': mes,
                            'max_temp': metrics['max_temp'],
                            'max_sd': metrics['max_sd'],
                            'mean_temp': current_mean_temp,
                            'max_diff_temp': max_diff_temp,
                            'mean_temp_count': metrics['mean_temp_count'],
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
                    "file": key,
                    "rows_processed": total_rows,
                    "months_updated": updated_months,
                    "alerts_sent": alerts_sent
                })
            }

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise Exception(f"Error processing file {key} from bucket {bucket}: {str(e)}")
    
    finally:
        if os.path.exists(local_filename):
            os.remove(local_filename)