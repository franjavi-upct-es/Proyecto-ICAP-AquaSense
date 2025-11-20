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
        print(f"Event: {json.dump(event, indent=2)}")

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

        # 3. Verificar alarmas de desviación
        check_deviation_alerts(weekly_data, key)

        # 4. Calcular métricas mensuales
        monthly_metrics = calculate_monthly_metrics(weekly_data)

        # 5. Almacenar en DynamoDB
        store_in_dynamodb(monthly_metrics)

        print("\n" + "=" * 70)
        print("PROCESAMIENTO COMPLETADO EXITOSAMENTE")
        print("=" * 70)
        print(f"Número de registros mensuales actualizados: {len(monthly_metrics)}")

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "CSV procesado exitosamente",
                    "file": key,
                    "monthly_records": len(monthly_metrics),
                    "weekly_records": len(weekly_data),
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
    date_formats = ["%Y/%m/%d", "%d/%m%Y", "%Y-%m-%d"]

    for row_num, row in enumerate(reader, start=2):  # Línea siguiente a header
        try:
            fecha_str = row["Fecha"].strip()

            # Intentar parsear la fecha con diferentes formatos
            fecha = None
            for fmt in date_formats:
                try:
                    fecha = datetime.strptime(fecha_str, fmt)
                    break
                except ValueError:
                    continue

            if not fecha:
                print(
                    f"ADVERTENCIA: No se pudo parsear fecha en línea {row_num}: {fecha_str}"
                )
                continue

            # Parsear valores numéricos
            media = float(row["Medias"])
            desviacion = float(row["Desviaciones"])

            weekly_data.append(
                {
                    "fecha": fecha,
                    "year": fecha.year,
                    "month": fecha.month,
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

def get_previous_month_max_temp(current_year, current_month):
    # Calcular mes anterior
    prev_month = current_month - 1
    prev_year = current_year
    if prev_month == 0:
        prev_month = 12
        prev_year -= 1

    prev_key = f"{prev_year}-{prev_month:02d}"

    try:
        # Buscamos la métrica 'temp' que contiene el artributo 'max_temp'
        response = table.get_item(
            Key={
                'month_year': prev_key,
                'metric_type': 'temp'
            }
        )
        if 'Item' in response:
            # DynamoDB devuelve Decimal, convertimos a float
            return float(response['Item'].get('max_temp', 0))
    except Exception as e:
        print(f"No se pudo obtener datos del mes {prev_key}: {e}")

    return None # No hay datos previos

# ============================================================================
# FUNCIONES DE CÁLCULO DE MÉTRICAS
# ============================================================================


def calculate_monthly_metrics(weekly_data: list) -> list:
    """
    Calcula las métricas mensuales requeridas por el proyecto.

    Requisitos del proyecto:
        a) Máxima desviación del conjunto de datos obtenidos durante el mes.
        b) Diferencia de la máxima temperatura del mes respecto a la máxima
           temperatura del mes anterior.

    Args:
        weekly_data: Lista de registros semanales

    Returns:
        Lista de diccionarios con métricas mensuales
            [{
                'month_year': str,
                'max_deviation': float,
                'avg_temperature': float,
                'max_temperature': float,
                'temp_diff': float,
                'record_count': int
            }, ...]
    """
    print("Calculando métricas mensuales...")

    # 1. Agrupar datos por mes/año
    monthly_groups = defaultdict(list)

    for record in weekly_data:
        month_year = f"{record['year']}-{record['month']:02d}"
        monthly_groups[month_year].append(record)

    # 2. Ordenar meses cronológicamente (importante para calcular las diferencias)
    sorted_months = sorted(monthly_groups.keys())

    print(f"Meses encontrados: {sorted_months}")

    # 3. Calcular métricas para cada mes
    monthly_metrics = []
    previous_max_temp = None

    for month_year in sorted_months:
        records = monthly_groups[month_year]

        # Calcular métricas del mes
        max_deviation = max(r["desviacion"] for r in records)
        avg_temperature = sum(r["media"] for r in records) / len(records)
        max_temperature = max(r["media"] for r in records)

        # 1. Intentar obtener max_temp del mes anterior de la variable local (si venía en el mismo CSV)
        prev_max_db = None
    
        # 2. Si no está en local, buscar en DynamoDB (caso de subida incremental)
        if previous_max_temp is None:
            prev_max_db = get_previous_month_max_temp(records[0]['year'], records[0]['month'])
        else:
            prev_max_db = previous_max_temp

        # 3. Calcular diferencia
        if prev_max_db is not None:
            temp_diff = max_temperature - prev_max_db
        else:
            temp_diff = 0.0 # Es el primer dato histórico que tenemos

        monthly_metrics.append(
            {
                "month_year": month_year,
                "max_deviation": max_deviation,
                "avg_temperature": avg_temperature,
                "max_temperature": max_temperature,
                "temp_diff": temp_diff,
                "record_count": len(records),
            }
        )

        # Logging detallado
        print(f"• {month_year}")
        print(f"\t- Max desviación: {max_deviation:.4f}ºC")
        print(f"\t- Temp media:     {avg_temperature:.2f}ºC")
        print(f"\t- Temp máxima:    {max_temperature:.2f}ºC")
        print(f"\t- Diferencia:     {temp_diff:+.2f}ºC")
        print(f"\t- Registros:      {len(records)}")

        # Actualizar para próxima iteración
        previous_max_temp = max_temperature

    print(f"Total meses procesados: {len(monthly_metrics)}")

    return monthly_metrics


# ============================================================================
# FUNCIONES DE ALMACENAMIENTO
# ============================================================================


def store_in_dynamodb(monthly_metrics: list):
    """
    Alamacena las métricas mensuales en DynamoDB

    Estructura de la tabla:
        - PK: month_year (ej: "2017-03")
        - SK: metric_type (ej: "temp", "sd", "maxdiff")
        - Atributos: value, max_temp, last_updated, record_count

    Args:
        monthly_metrics: Lista con métricas mensuales calculadas
    """
    print("Almacenando datos en DynamoDB...")

    timestamp = datetime.now().isoformat()
    total_updates = 0

    for metrics in monthly_metrics:
        month_year = metrics["month_year"]

        # Convertir floats a Decimal (requerido por DynamoDB)
        max_dev_decimal = Decimal(str(metrics["max_deviation"]))
        avg_temp_decimal = Decimal(str(metrics["avg_temperature"]))
        max_temp_decimal = Decimal(str(metrics["max_temperature"]))
        temp_diff_decimal = Decimal(str(metrics["temp_diff"]))

        try:
            # 1. Almacenar máxiam desviación (sd = standard deviation)
            table.put_item(
                Item={
                    "month_year": month_year,
                    "metric_type": "sd",
                    "value": max_dev_decimal,
                    "last_updated": timestamp,
                    "record_count": metrics["record_count"],
                }
            )

            # 2. Almacenar temperatura media mensaul (temp)
            table.put_item(
                Item={
                    "month_year": month_year,
                    "metric_type": "temp",
                    "value": avg_temp_decimal,
                    "max_temp": max_temp_decimal,
                    "last_updated": timestamp,
                    "record_count": metrics["record_count"],
                }
            )

            # 3. Almacenar diferencia de máxima temperatura (maxdiff)
            table.put_item(
                Item={
                    "month_year": month_year,
                    "metric_type": "maxdiff",
                    "value": temp_diff_decimal,
                    "max_temp": max_temp_decimal,
                    "last_updated": timestamp,
                    "record_count": metrics["record_count"],
                }
            )

            total_updates += 3
            print(f"{month_year}: 3 métricas almacenadas")

        except Exception as e:
            print(f"ERROR almacenando {month_year}: {str(e)}")

    print(f"Total actualizaciones en DynamoDB: {total_updates}")

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
