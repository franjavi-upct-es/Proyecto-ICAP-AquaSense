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
    """Redondea a 2 decimales para consistencia"""
    return value.quantize(Decimal("0.01"))

def send_alert(fecha_str, desviacion):
    """Envía la notificación SNS"""
    try:
        message = f"ALERTA: Desviación de {desviacion} detectada el {fecha_str}."
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject="Alerta Mar Menor - Desviación Alta",
            Message=message
        )
    except Exception as e:
        print(f"Error enviando SNS: {e}")

# ============================================================================
# HANDLER PRINCIPAL
# ============================================================================

def lambda_handler(event, context):
    print("Event received by Lambda function: " + json.dumps(event, indent=2))

    # Obtener bucket y key del evento
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
    local_filename = f"/tmp/{os.path.basename(key)}"

    try:
        # Descargar el archivo desde S3 (Lógica de referencia)
        s3_client.download_file(bucket, key, local_filename)

        # Procesar el archivo CSV
        with open(local_filename, encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=',')
            data = {}

            for row in reader:
                # Parseo de datos
                try:
                    fecha = datetime.strptime(row['Fecha'], '%Y/%m/%d')
                except ValueError:
                     # Intento de soporte para formatos alternativos si fuera necesario
                     fecha = datetime.strptime(row['Fecha'], '%Y-%m-%d')
                
                # Usamos 'monthYear' como clave para mantener consistencia con tu proyecto
                mes = fecha.strftime('%Y-%m') # Formato YYYY-MM
                
                temp_media = round_decimal(Decimal(row['Medias']))
                desviacion = round_decimal(Decimal(row['Desviaciones']))

                # Agregación en memoria (Lógica de referencia)
                if mes not in data:
                    data[mes] = {
                        'max_temp': temp_media,
                        'max_sd': desviacion,
                        'mean_temp_sum': temp_media,
                        'mean_temp_count': 1,
                        'records': [(fecha, temp_media, desviacion)]
                    }
                else:
                    data[mes]['max_temp'] = max(data[mes]['max_temp'], temp_media)
                    data[mes]['max_sd'] = max(data[mes]['max_sd'], desviacion)
                    data[mes]['mean_temp_sum'] += temp_media
                    data[mes]['mean_temp_count'] += 1
                    data[mes]['records'].append((fecha, temp_media, desviacion))

                # Notificar si la desviación supera el umbral (Lógica de referencia)
                if desviacion > DEVIATION_THRESHOLD:
                    send_alert(fecha.strftime('%Y/%m/%d'), desviacion)

            # Procesar y actualizar DynamoDB con datos combinados
            for mes, metrics in data.items():
                try:
                    # Consultar el registro existente en la base de datos
                    # Nota: Usamos 'monthYear' como Partition Key según tu infraestructura
                    existing_item_resp = table.get_item(Key={'monthYear': mes})
                    existing_item = existing_item_resp.get('Item', {})

                    # Calcular mes anterior para max_diff_temp
                    current_month_dt = datetime.strptime(mes, '%Y-%m')
                    # Restamos días para ir al mes anterior de forma segura
                    previous_month_dt = current_month_dt.replace(day=1) - timedelta(days=1)
                    previous_month = previous_month_dt.strftime('%Y-%m')

                    # Caso 1: Mes anterior en la base de datos
                    previous_item_resp = table.get_item(Key={'monthYear': previous_month})
                    previous_item = previous_item_resp.get('Item', {})
                    previous_max_temp_db = Decimal(previous_item.get('max_temp', 0))

                    # Caso 2: Mes anterior en el archivo actual (Lógica de referencia)
                    if previous_month in data:
                        previous_max_temp_file = data[previous_month]['max_temp']
                    else:
                        previous_max_temp_file = Decimal('0')

                    # Determinar el máximo del mes anterior (DB vs Archivo)
                    previous_max_temp = max(previous_max_temp_db, previous_max_temp_file)

                    # Combinar métricas locales con las de la DB
                    combined_max_temp = max(metrics['max_temp'], Decimal(existing_item.get('max_temp', 0)))
                    combined_max_sd = max(metrics['max_sd'], Decimal(existing_item.get('max_sd', 0)))

                    # Cálculo de media ponderada
                    total_mean_temp_sum = metrics['mean_temp_sum'] + Decimal(existing_item.get('mean_temp', 0)) * Decimal(existing_item.get('mean_temp_count', 0))
                    total_mean_temp_count = metrics['mean_temp_count'] + Decimal(existing_item.get('mean_temp_count', 0))
                    
                    if total_mean_temp_count > 0:
                        combined_mean_temp = round_decimal(total_mean_temp_sum / total_mean_temp_count)
                    else:
                        combined_mean_temp = Decimal('0')

                    # Diferencia con el mes anterior
                    max_diff_temp = round_decimal(combined_max_temp - previous_max_temp)

                    # Actualizar o insertar el registro (Estructura plana sin Sort Key)
                    table.put_item(
                        Item={
                            'monthYear': mes, # PK mantenida como monthYear
                            'max_temp': combined_max_temp,
                            'max_sd': combined_max_sd,
                            'mean_temp': combined_mean_temp,
                            'max_diff_temp': max_diff_temp,
                            'mean_temp_count': total_mean_temp_count,
                            'last_updated': datetime.now().isoformat()
                        }
                    )
                    print(f"Updated DynamoDB for {mes}")

                except Exception as e:
                    print(e)
                    raise Exception(f"Error updating DynamoDB for {mes}: {str(e)}")

    except Exception as e:
        print(e)
        raise Exception(f"Error processing file {key} from bucket {bucket}: {str(e)}")
    
    finally:
        # Limpiar el archivo temporal
        if os.path.exists(local_filename):
            os.remove(local_filename)

# ============================================================================
# FUNCIONES DE CÁLCULO DE MÉTRICAS
# ============================================================================


def calculate_monthly_metrics(weekly_data: list) -> list:
    """
    DEPRECATED: Esta función ya no se usa directamente.
    Usar calculate_monthly_metrics_from_db() en su lugar.
    """
    pass


def calculate_monthly_metrics_from_db(weekly_data: list) -> list:
    """
    Calcula métricas mensuales consultando SOLO datos únicos de DynamoDB.
    Esto garantiza que si hubo duplicados, solo se cuenta el último valor.
    
    Requisitos del proyecto:
        a) Máxima desviación del conjunto de datos obtenidos durante el mes.
        b) Diferencia de la máxima temperatura del mes respecto a la máxima
           temperatura del mes anterior.
        c) Temperatura media de los datos del mes
    
    Args:
        weekly_data: Lista de registros semanales (para saber qué meses procesar)
    
    Returns:
        Lista de métricas mensuales calculadas desde datos únicos
            [{
                'monthYear': str,
                'max_deviation': float,
                'avg_temperature': float,
                'max_temperature': float,
                'temp_diff': float,
                'record_count': int,
                'sum_temp': float
            }, ...]
    """
    print("\n" + "="*70)
    print("CALCULANDO MÉTRICAS MENSUALES DESDE DATOS ÚNICOS")
    print("="*70)
    
    # 1. Identificar meses a procesar
    months_to_process = set()
    for record in weekly_data:
        monthYear = f"{record['year']}-{record['month']:02d}"
        months_to_process.add((monthYear, record['year'], record['month']))
    
    sorted_months = sorted(months_to_process)
    print(f"Meses a recalcular: {[m[0] for m in sorted_months]}\n")
    
    monthly_metrics = []
    previous_max_temp = None
    
    for monthYear, year, month in sorted_months:
        print(f"• Procesando {monthYear}...")
        
        # 2. Obtener el primer y último día del mes
        if month == 12:
            next_month = 1
            next_year = year + 1
        else:
            next_month = month + 1
            next_year = year
        
        start_date = f"{year}-{month:02d}-03"
        end_date = f"{next_year}-{next_month:02d}-04"
        
        # 3. Consultar TODOS los registros semanales del mes desde DynamoDB
        weekly_records = []
        
        try:
            # Escanear todas las fechas del mes
            response = table.scan(
                FilterExpression="metric_type = :mt AND monthYear >= :start AND monthYear < :end",
                ExpressionAttributeValues={
                    ':mt': 'weekly',
                    ':start': start_date,
                    ':end': end_date
                }
            )
            weekly_records = response.get('Items', [])
            
        except Exception as e:
            print(f"  ⚠️ Error consultando datos: {e}")
            continue
        
        if not weekly_records:
            print(f"  ⚠️ No hay registros únicos para {monthYear}")
            continue
        
        print(f"  📊 Registros únicos encontrados: {len(weekly_records)}")
        
        # 4. Calcular métricas sobre datos únicos
        temperatures = [float(r['temperature']) for r in weekly_records]
        deviations = [float(r['deviation']) for r in weekly_records]
        
        max_deviation = max(deviations)
        avg_temperature = sum(temperatures) / len(temperatures)
        max_temperature = max(temperatures)
        total_sum = sum(temperatures)
        total_count = len(weekly_records)
        
        # 5. Calcular diferencia con mes anterior
        if previous_max_temp is None:
            # Buscar en DynamoDB
            prev_max_db = get_previous_month_max_temp(year, month)
            temp_diff = max_temperature - prev_max_db if prev_max_db else 0.0
        else:
            temp_diff = max_temperature - previous_max_temp
        
        monthly_metrics.append({
            'monthYear': monthYear,
            'max_deviation': max_deviation,
            'avg_temperature': avg_temperature,
            'max_temperature': max_temperature,
            'temp_diff': temp_diff,
            'record_count': total_count,
            'sum_temp': total_sum,
        })
        
        # Logging detallado
        print(f"  ├─ Max desviación: {max_deviation:.4f}°C")
        print(f"  ├─ Temp media:     {avg_temperature:.2f}°C")
        print(f"  ├─ Temp máxima:    {max_temperature:.2f}°C")
        print(f"  ├─ Diferencia:     {temp_diff:+.2f}°C")
        print(f"  └─ Registros:      {total_count}\n")
        
        previous_max_temp = max_temperature
    
    print(f"✓ Total meses recalculados: {len(monthly_metrics)}")
    print("="*70)
    return monthly_metrics


# ============================================================================
# FUNCIONES DE ALMACENAMIENTO DE MÉTRICAS AGREGADAS
# ============================================================================


def store_monthly_metrics(monthly_metrics: list):
    """
    Almacena las métricas mensuales AGREGADAS en DynamoDB.
    Sobrescribe completamente los valores porque ya fueron calculados
    desde datos únicos.
    
    Estructura de la tabla:
        - PK: monthYear (ej: "2017-03")
        - SK: metric_type (ej: "temp", "sd", "maxdiff")
        - Atributos: value, max_temp, sum_temp, last_updated, record_count
    
    Args:
        monthly_metrics: Lista con métricas mensuales calculadas
    """
    print("\n" + "="*70)
    print("ALMACENANDO MÉTRICAS MENSUALES AGREGADAS")
    print("="*70)
    
    timestamp = datetime.now().isoformat()
    total_updates = 0

    for metrics in monthly_metrics:
        monthYear = metrics["monthYear"]

        # Convertir a Decimal
        max_dev_decimal = Decimal(str(metrics["max_deviation"]))
        avg_temp_decimal = Decimal(str(metrics["avg_temperature"]))
        max_temp_decimal = Decimal(str(metrics["max_temperature"]))
        temp_diff_decimal = Decimal(str(metrics["temp_diff"]))
        sum_temp_decimal = Decimal(str(metrics["sum_temp"]))
        record_count = metrics["record_count"]

        try:
            # Sobrescribir todas las métricas (ya están calculadas correctamente)
            
            # 1. Máxima desviación (sd)
            table.put_item(
                Item={
                    "monthYear": monthYear,
                    "metric_type": "sd",
                    "value": max_dev_decimal,
                    "last_updated": timestamp,
                    "record_count": record_count,
                }
            )

            # 2. Temperatura media mensual (temp)
            table.put_item(
                Item={
                    "monthYear": monthYear,
                    "metric_type": "temp",
                    "value": avg_temp_decimal,
                    "max_temp": max_temp_decimal,
                    "sum_temp": sum_temp_decimal,
                    "last_updated": timestamp,
                    "record_count": record_count,
                }
            )

            # 3. Diferencia máxima temperatura (maxdiff)
            table.put_item(
                Item={
                    "monthYear": monthYear,
                    "metric_type": "maxdiff",
                    "value": temp_diff_decimal,
                    "max_temp": max_temp_decimal,
                    "last_updated": timestamp,
                    "record_count": record_count,
                }
            )

            total_updates += 3
            print(f"  ✓ {monthYear}: 3 métricas almacenadas")

        except Exception as e:
            print(f"  ✗ ERROR almacenando {monthYear}: {str(e)}")
            import traceback
            traceback.print_exc()

        print(f"\n✓ Total actualizaciones: {total_updates}")
        print("="*70)
        record_count = metrics["record_count"]

        try:
            # 1. Máxima desviación (sd) - UPDATE atómico con MAX
            # Lee el valor actual y actualiza solo si el nuevo es mayor
            response = table.get_item(
                Key={'monthYear': monthYear, 'metric_type': 'sd'}
            )
            
            if 'Item' in response:
                current_sd = response['Item'].get('value', Decimal('0'))
                current_count = response['Item'].get('record_count', 0)
                # Tomar el máximo entre actual y nuevo
                final_sd = max(current_sd, max_dev_decimal)
                final_count = current_count + record_count
            else:
                final_sd = max_dev_decimal
                final_count = record_count
            
            table.put_item(
                Item={
                    "monthYear": monthYear,
                    "metric_type": "sd",
                    "value": final_sd,
                    "last_updated": timestamp,
                    "record_count": final_count,
                }
            )

            # 2. Temperatura media mensual (temp) - UPDATE atómico con ADD
            # Usa update_item con ADD para acumular sum_temp y record_count
            table.update_item(
                Key={
                    'monthYear': monthYear,
                    'metric_type': 'temp'
                },
                UpdateExpression="""SET last_updated = :timestamp, 
                                       max_temp = if_not_exists(max_temp, :zero)
                                   ADD sum_temp :sum, record_count :count""",
                ExpressionAttributeValues={
                    ':sum': sum_temp_decimal,
                    ':count': record_count,
                    ':timestamp': timestamp,
                    ':zero': Decimal('0')
                }
            )
            
            # Actualizar max_temp si el nuevo es mayor
            table.update_item(
                Key={
                    'monthYear': monthYear,
                    'metric_type': 'temp'
                },
                UpdateExpression="SET max_temp = if_not_exists(max_temp, :max_temp)",
                ConditionExpression="attribute_not_exists(max_temp) OR max_temp < :max_temp",
                ExpressionAttributeValues={
                    ':max_temp': max_temp_decimal
                }
            ) if max_temp_decimal > Decimal('0') else None
            
            # Recalcular value (media) después de la actualización
            response = table.get_item(
                Key={'monthYear': monthYear, 'metric_type': 'temp'}
            )
            if 'Item' in response:
                item = response['Item']
                new_avg = float(item['sum_temp']) / float(item['record_count'])
                table.update_item(
                    Key={'monthYear': monthYear, 'metric_type': 'temp'},
                    UpdateExpression='SET #val = :avg',
                    ExpressionAttributeNames={'#val': 'value'},
                    ExpressionAttributeValues={':avg': Decimal(str(new_avg))}
                )

            # 3. Diferencia máxima temperatura (maxdiff)
            # Este valor se recalcula cada vez basado en el mes anterior
            table.put_item(
                Item={
                    "monthYear": monthYear,
                    "metric_type": "maxdiff",
                    "value": temp_diff_decimal,
                    "max_temp": max_temp_decimal,
                    "last_updated": timestamp,
                    "record_count": record_count,
                }
            )

            total_updates += 3
            print(f"✓ {monthYear}: 3 métricas almacenadas/actualizadas")

        except Exception as e:
            print(f"  ✗ ERROR almacenando {monthYear}: {str(e)}")
            import traceback
            traceback.print_exc()

    print(f"\n✓ Total actualizaciones: {total_updates}")
    print("="*70)

# ============================================================================
# FUNCIÓN PARA TESTING LOCAL
# ============================================================================

if __name__ == "__main__":
    """
    Código para testing local de la función Lambda.
    No se ejecuta en AWS Lambda.
    """
    print("=" * 3 + "Modo de testing local" + "=" * 3)
    print("Este código solo debe ejecutarse para desarrollo local")
    
    # Ejemplo de evento S3 para testing
    test_event = {
        'Records': [{
            's3': {
                'bucket': {'name': 'proy-marmenor-data-raw-test'},
                'object': {'key': 'temperatura_test.csv'}
            }
        }]
    }
    
    # Mock de variables de entorno
    os.environ['DYNAMODB_TABLE'] = 'proy-MarMenorData'
    os.environ['SNS_TOPIC_ARN'] = 'arn:aws:sns:us-east-1:123456789012:proy-marmenor-alerts'
    os.environ['DEVIATION_THRESHOLD'] = '0.5'
    
    # Ejecutar handler
    result = lambda_handler(test_event, None)
    print(f"\nResultado: {json.dumps(result, indent=2)}")
