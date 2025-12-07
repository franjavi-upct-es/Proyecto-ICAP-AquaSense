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
from datetime import datetime
from io import StringIO
from decimal import Decimal
from collections import defaultdict

# ============================================================================
# CONFGIURACIÓN Y CLIENTES AWS
# ============================================================================

# Clientes AWS
s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
sns_client = boto3.client("sns")

# Variables de entorno
DYNAMODB_TABLE = os.environ["DYNAMODB_TABLE"]
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
DEVIATION_THRESHOLD = float(os.environ.get("DEVIATION_THRESHOLD", "0.5"))

# Tabla DynamoDB
table = dynamodb.Table(DYNAMODB_TABLE)

# ============================================================================
# FUNCIÓN PRINCIPAL - HANDLER
# ============================================================================


def lambda_handler(event, context):
    """
    Handler principal de Lambda.

    Se ejecuta cuando se sube un archivo CSV a s3.

    Args:
        event: Evento de S3 con información del archivo
        context: Contexto de ejecución de Lambda

    Returns:
        dict: Respuesta con statusCode y mensaje
    """
    try:
        print("=" * 70)
        print("INICIO DEL PROCESAMIENTO - AquaSenseCloud")
        print("=" * 70)
        print(f"Event: {json.dumps(event, indent=2)}")

        # Extraer información del archivo S3
        bucket = event["Records"][0]["s3"]["bucket"]["name"]
        key = event["Records"][0]["s3"]["object"]["key"]

        print("Procesando archivo:")
        print(f"\tBucket: {bucket}")
        print(f"\tKey: {key}")

        # 1. Leer archivo CSV de S3
        csv_data = read_csv_from_s3(bucket, key)

        # 2. Parsear datos del CSV
        weekly_data = parse_csv_data(csv_data)

        # 3. PRIMERO: Almacenar datos semanales individuales (sobrescribe duplicados)
        store_weekly_data(weekly_data)

        # 4. Verificar alarmas de desviación
        check_deviation_alerts(weekly_data, key)

        # 5. Calcular métricas mensuales desde DynamoDB (datos únicos)
        monthly_metrics = calculate_monthly_metrics_from_db(weekly_data)

        # 6. Almacenar métricas agregadas en DynamoDB
        store_monthly_metrics(monthly_metrics)

        print("\n" + "=" * 70)
        print("PROCESAMIENTO COMPLETADO EXITOSAMENTE")
        print("=" * 70)
        print(f"Registros semanales procesados: {len(weekly_data)}")
        print(f"Meses actualizados: {len(monthly_metrics)}")

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "CSV procesado exitosamente",
                    "file": key,
                    "weekly_records": len(weekly_data),
                    "monthly_records": len(monthly_metrics),
                }
            ),
        }

    except Exception as e:
        print("=" * 70)
        print(f"ERROR EN EL PROCESAMIENTO: {str(e)}")
        print("=" * 70)

        import traceback

        traceback.print_exc()

        return {
            "statusCode": 500,
            "body": json.dumps(
                {"error": str(e), "file": key if "key" in locals() else "unknown"}
            ),
        }


# ============================================================================
# FUNCIONES DE LECTURA Y PARSING
# ============================================================================


def read_csv_from_s3(bucket: str, key: str) -> str:
    """
    Lee el contenido de un archivo CSV desde S3.

    Args:
        bucket: Nombre del bucket S3
        key: Key del objeto en S3

    Returns:
        Contenido del archivo CSV

    Raises:
        Exception: Si hay algún error al leer el archivo
    """
    try:
        print("Leyendo archivo desde S3")
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response["Body"].read().decode("utf-8")

        lines = content.count("\n")
        print(f"Archivo leído con éxito ({lines} líneas)")

        return content
    except Exception as e:
        raise Exception(f"Error leyendo archivo desde S3: {str(e)}")


def parse_csv_data(csv_content: str) -> list:
    """
    Parsea el contenido CSV y devuelve una lista de registros semanales.

    Formato esperado del CSV:
        Fecha,Medias,Desviaciones
        2017/03/22,16.78,0.287
        2017/03/30,17.32,0.403

    Args:
        csv_content: Contenido del archivo CSV

    Returns:
        Lista de diccionarios con datos parseados
            [{
                'fecha': datetime,
                'year': int,
                'month': int,
                'media': float,
                'desviacion': float
            }, ...]
    """
    weekly_data = []
    csv_file = StringIO(csv_content)
    reader = csv.DictReader(csv_file)

    print("Parseando datos CSV...")

    # Formatos de fecha esperados
    date_formats = ["%Y/%m/%d"]

    for row_num, row in enumerate(reader, start=2):  # Línea siguiente a header
        try:
            fecha_str = row["Fecha"].strip()

            # Intentar parsear la fecha con diferentes formatos
            try:
                fecha = datetime.strptime(fecha_str, "%Y/%m/%d")
                # Usar directamente el mes de la fecha, sin ajustes
                mes = fecha.month
                año = fecha.year
            except Exception as e:
                print(f"No se pudo parsear fecha en línea {row_num}: {fecha_str}")
                continue

            # Parsear valores numéricos
            media = float(row["Medias"])
            desviacion = float(row["Desviaciones"])

            weekly_data.append(
                {
                    "fecha": fecha,
                    "year": año,
                    "month": mes,
                    "media": media,
                    "desviacion": desviacion,
                }
            )

        except KeyError as e:
            print(f"ADVERTENCIA: Columna faltante en línea {row_num}: {e}")
            continue
        except ValueError as e:
            print(f"ADVERTENCIA: Error de conversión en línea {row_num}: {e}")
            continue
        except Exception as e:
            print(f"ADVERTENCIA: Error inesperado en línea {row_num}: {e}")
            continue

    print(f"Total de registros semanales parseados: {len(weekly_data)}")

    if len(weekly_data) == 0:
        raise Exception("No se pudieron parsear registros válidos del CSV")

    return weekly_data



# ============================================================================
# FUNCIONES DE DETECCIÓN DE ALARMAS
# ============================================================================


def check_deviation_alerts(weekly_data: list, filename: str):
    """
    Verifica si alguna desviación semanal supera el umbral y envía alarmas.

    Requisito del proyecto:
        "Necesitan saber cuándo la desviación de la temperatura semanal
        del agua del Mar Menor es superior a 0.5"

    Args:
        weekly_data: lista de registros semanales
        filename: Nombre del archivo procesado
    """
    print(f"Verificando alarmas (umbral: {DEVIATION_THRESHOLD}°C)...")

    alerts_sent = 0

    for record in weekly_data:
        if record["desviacion"] > DEVIATION_THRESHOLD:
            send_alert(record, filename)
            alerts_sent += 1

    if alerts_sent > 0:
        print(f"Total alertas enviadas: {alerts_sent}")
    else:
        print("No se detectaron desviaciones que superen el umbral")


def send_alert(record: dict, filename: str):
    """
    Envía una alerta por SNS cuando se detecta una desviación alta.

    Args:
        record: Registro con desviación alta
        filename: Nombre del archivo procesado
    """
    try:
        fecha = record["fecha"].strftime("%Y-%m-%d")
        desviacion = record["desviacion"]
        media = record["media"]

        subject = "⚠️ Alarma Mar Menor - Desviación Alta Detectada"

        message = f"""
⚠️ ALERTA DE TEMPERATURA - MAR MENOR
=========================================

Se ha detectado una desviación de temperatura superior al umbral establecido.

📊 DETALLES DE LA ALERTA
-----------------------------------------
- Fecha del registro:   {fecha}
- Desviación detectada: {desviacion:.4f}°C ⚠️
- Umbral configurado:   {DEVIATION_THRESHOLD}°C
- Temperatura media:    {media:.2f}°C

ℹ️ INFORMACIÓN ADICIONAL
-----------------------------------------
- Archivo procesado:    {filename}
- Timestamp:            {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
- Sistema:              AquaSenseCloud Lambda Function

🔍 INTERPRETACIÓN
-----------------------------------------
Una desviación de {desviacion:.4f}°C indica alta variabilidad en las
temperaturas del agua. Posibles causas:
  • Cambios climáticos bruscos
  • Fenómenos meteorológicos
  • Alteraciones en la laguna

> Se recomienda revisar los datos semanales detallados.

--
Sistema AquaSenseCloud
Infraestructuras para la Computación de Altas Prestaciones - UPCT
        """.strip()

        response = sns_client.publish(
            TopicArn=SNS_TOPIC_ARN, Subject=subject, Message=message
        )

        print(f"Alerta enviada para {fecha} (desv: {desviacion:.4f}°C)")
        print(f"SNS MessageId: {response['MessageId']}")

    except Exception as e:
        print(f"ERROR enviando alerta SNS: {str(e)}")

def get_current_month_max_temp(current_year, current_month):
    curr_key = f"{current_year}-{current_month:02d}"
    try:
        # Buscamos la métrica 'temp' que contiene el artributo 'max_temp'
        response = table.get_item(
            Key={
                'monthYear': curr_key,
                'metric_type': 'temp'
            }
        )
        if 'Item' in response:
            # DynamoDB devuelve Decimal, convertimos a float
            return float(response['Item'].get('max_temp', 0))
    except Exception as e:
        print(f"No se pudo obtener datos del mes {curr_key}: {e}")
    return None # No hay datos previos

def get_previous_month_max_temp(current_year, current_month):
    # Calcular mes anterior
    prev_month = current_month - 1
    prev_year = current_year
    if prev_month == 0:
        prev_month = 12
        prev_year -= 1
    return get_current_month_max_temp(prev_year, prev_month)

def get_current_month_max_sd(current_year, current_month):
    curr_key = f"{current_year}-{current_month:02d}"
    try:
        # Buscamos la métrica 'temp' que contiene el artributo 'max_temp'
        response = table.get_item(
            Key={
                'monthYear': curr_key,
                'metric_type': 'sd'
            }
        )
        if 'Item' in response:
            # DynamoDB devuelve Decimal, convertimos a float
            return float(response['Item'].get('value', 0))
    except Exception as e:
        print(f"No se pudo obtener datos del mes {curr_key}: {e}")
    return None # No hay datos previos

def get_current_month_avg_temp(current_year, current_month):
    curr_key = f"{current_year}-{current_month:02d}"
    try:
        # Buscamos la métrica 'temp' que contiene el artributo 'sum_temp'
        response = table.get_item(
            Key={
                'monthYear': curr_key,
                'metric_type': 'temp'
            }
        )
        if 'Item' in response:
            # DynamoDB devuelve Decimal, convertimos a float
            # Devolvemos suma acumulada y contador para calcular media correctamente
            sum_temp = float(response['Item'].get('sum_temp', 0))
            count = float(response['Item'].get('record_count', 0))
            return sum_temp, count
    except Exception as e:
        print(f"No se pudo obtener datos del mes {curr_key}: {e}")

    return None, None # No hay datos previos

# ============================================================================
# FUNCIONES DE ALMACENAMIENTO DE DATOS SEMANALES
# ============================================================================

def store_weekly_data(weekly_data: list):
    """
    Almacena cada registro semanal individualmente en DynamoDB.
    Si una fecha ya existe, se SOBRESCRIBE con el nuevo valor.
    
    Estructura:
        - PK: fecha en formato 'YYYY-MM-DD'
        - SK: 'weekly'
        - Atributos: temperature, deviation, year, month
    
    Args:
        weekly_data: Lista de registros semanales parseados
    """
    print("\n" + "="*70)
    print("ALMACENANDO REGISTROS SEMANALES INDIVIDUALES")
    print("="*70)
    
    timestamp = datetime.now().isoformat()
    stored_count = 0
    updated_count = 0
    
    for record in weekly_data:
        fecha_key = record['fecha'].strftime('%Y-%m-%d')
        
        # Verificar si ya existe (para logging)
        try:
            response = table.get_item(
                Key={
                    'monthYear': fecha_key,
                    'metric_type': 'weekly'
                }
            )
            if 'Item' in response:
                old_temp = float(response['Item']['temperature'])
                updated_count += 1
                print(f"  ⟳ Actualizando {fecha_key}: {old_temp:.2f}°C → {record['media']:.2f}°C")
            else:
                stored_count += 1
                print(f"  ✓ Nuevo registro {fecha_key}: {record['media']:.2f}°C")
        except:
            stored_count += 1
            print(f"  ✓ Nuevo registro {fecha_key}: {record['media']:.2f}°C")
        
        # put_item SOBRESCRIBE automáticamente si la clave ya existe
        table.put_item(
            Item={
                'monthYear': fecha_key,  # PK: fecha exacta
                'metric_type': 'weekly',  # SK: tipo de dato
                'temperature': Decimal(str(record['media'])),
                'deviation': Decimal(str(record['desviacion'])),
                'year': record['year'],
                'month': record['month'],
                'last_updated': timestamp
            }
        )
    
    print(f"\n📊 RESUMEN DE ALMACENAMIENTO SEMANAL:")
    print(f"  • Registros nuevos:       {stored_count}")
    print(f"  • Registros actualizados: {updated_count}")
    print(f"  • Total procesado:        {len(weekly_data)}")
    print("="*70)

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
        
        start_date = f"{year}-{month:02d}-01"
        end_date = f"{next_year}-{next_month:02d}-01"
        
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
