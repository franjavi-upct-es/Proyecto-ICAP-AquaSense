#!/bin/bash
# Script de configuraci??n e inicio de Docker para AquaSenseCloud API

# --- 1. INSTALACI??N Y CONFIGURACI??N DE DOCKER ---
yum update -y
yum -y install docker
systemctl start docker
systemctl enable docker
usermod -a -G docker ec2-user

# Esperar un momento para que Docker se inicialice (necesario en algunos entornos)
sleep 5

# Crear directorio de trabajo y moverse a ??l
mkdir -p /app
cd /app

# --- 2. CREACI??N DE ARCHIVOS REQUERIDOS ---

# 2.1. Crear requirements.txt
cat <<EOF > requirements.txt
# Paquetes para AquaSenseCloud API
# Python 3.11+
# Proyecto: Infraestructuras para la Computaci??n de Altas Prestaciones - UPCT

# Framework web
Flask==3.0.0
Werkzeug==3.0.1

# AWS SDK para Python
boto3==1.34.0
botocore==1.34.0

# Servidor WSGI para producci??n (usado en Docker)
gunicorn==21.2.0

# Utilidades
python-dotenv==1.0.0

# Para desarrollo local
pytest==7.4.3
requests==2.31.0
EOF

# 2.2. Crear Dockerfile
cat <<EOF > Dockerfile
# Dockerfile para AquaSenseCloud API
# Imagen optimizada para ECS Fargate

# Imagen base oficial de Python
FROM python:3.11-slim

# Metadata
LABEL maintainer="aquasensecloud@upct.es"
LABEL description="API REST para consultar datos del Mar Menor"
LABEL version="1.0"
LABEL project="AquaSenseCloud - ICAP UPCT"

# Variables de entorno
ENV PYTHONUNBUFFERED=1 \
  PYTHONDONTWRITEBYTECODE=1 \
  PORT=8080 \
  AWS_DEFAULT_REGION=us-east-1

# Instalar dependencias del sistema necesarias
RUN apt-get update && \
  apt-get install -y --no-install-recommends \
  curl \
  && rm -rf /var/lib/apt/lists/*

# Crear directorio de trabajo
WORKDIR /app

# Copiar requirements primero (para aprovechar cache de Docker)
COPY requirements.txt .

# Instalar dependencias de Python
RUN pip install --no-cache-dir --upgrade pip && \
  pip install --no-cache-dir -r requirements.txt

# Copiar c??digo de la aplicaci??n
COPY aquasense.py .

# Cerar usuario no-root para seguridad
RUN useradd -m -u 1000 appuser && \
  chown -R appuser:appuser /app

# Cambiar a usuario no-root
USER appuser

# Exponer puerto
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \ 
  CMD curl -f http://localhost:8080/health || exit 1

# Comando de inicio (usando gunicorn para producci??n)
CMD [ "gunicorn", "--bind", "0.0.0.0:8080", "--workers", "2", "--threads", "4", "--timeout", "60", "--access-logfile", "-", "--error-logfile", "-", "aquasense:app" ]

EOF

# 2.3. Crear aquasense.py (??C??DIGO PYTHON COMPLETO!)
# Se usa 'PYTHONEOF' para asegurar que el contenido se escribe textualmente.
cat <<'PYTHONEOF' > aquasense.py
"""
AquaSense API REST - Servidor Web para consultar datos del Mar Menor

Autor: Equipo AquaSenseCloud

Descripci??n:
    API REST que proporciona acceso a las estad??sticas de temperatura
    del Mar Menor almacenadas en DynamoDB

Endpoints:
    GET /health - Health check del servidor
    GET /maxdiff?month=M&year=Y - Diferencia de temperatura m??xima mensual
    GET /sd?month=M&year=Y - M??xima desviaci??n est??ndar mensual
    GET /temp?month=M&year=Y - Temperatura media mensual
    GET /months - Lista de todos los meses con datos disponibles

Requisitos:
    - Flask 3.0.0
    - boto3 1.34.0
    - Python 3.11+

Variables de Entorno:
    - PORT: Puerto del servidor (default: 8080)
    - DYNAMODB_TABLE: Nombre de la tabla DynamoDB
    - AWS_REGION: Regi??n AWS (default: us-east-1)
"""

from flask import Flask, request, jsonify
import boto3
import os
from decimal import Decimal
import logging

# =======================================
# CONFIGURACI??N
# =======================================

# Configuraci??n de logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Inicializaci??n de Flask
app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False  # Mantener el orden de las claves en el JSON

# Cliente DynamoDB
dynamodb = boto3.resource(
    "dynamodb", region_name=os.environ.get("AWS_REGION", "us-east-1")
)
table_name = os.environ.get("DYNAMODB_TABLE", "Measures")
table = dynamodb.Table(table_name)

logger.info(f"???? AquaSenseCloud API iniciada")
logger.info(f"???? DynamoDB Table: {table_name}")

# =======================================
# FUNCIONES AUXILIARES
# =======================================


def decimal_to_float(obj):
    """
    Convierte objetos Decimal de DynamoDB a float para serializaci??n JSON.

    Args:
        obj: Objeto que puede contener Decimals

    Returns:
        Objeto con Decimals convertidos a float
    """
    if isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, dict):
        return {k: decimal_to_float(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [decimal_to_float(item) for item in obj]
    return obj


def validate_parameters(request):
    """
    Valida los par??metros month y year de la request

    Args:
        request: Objeto request de Flask

    Returns:
        tuple: (month, year, error_response)
            - Si es v??lido: (int, int, None)
            - Si da error: (None, None, (json_response, status_code))
    """
    try:
        # Obtener par??metros
        month_str = request.args.get("month")
        year_str = request.args.get("year")

        # Verificar que existan
        if not month_str or not year_str:
            return (
                None,
                None,
                (
                    jsonify(
                        {
                            "error": "Par??metros faltantes",
                            "message": 'Los par??metros "month" y "year" son obligatorios',
                            "ejemplo": "/maxdiff?month=3&year=2017",
                        }
                    ),
                    400,
                ),
            )

        # Convertir a enteros
        month = int(month_str)
        year = int(year_str)

        # Validar rangos
        if not (1 <= month <= 12):
            return (
                None,
                None,
                (
                    jsonify(
                        {
                            "error": "Mes inv??lido",
                            "message": "El mes debe estar entre 1 y 12",
                            "recibido": month,
                        }
                    ),
                    400,
                ),
            )

        if not (2000 <= year <= 2100):
            return (
                None,
                None,
                (
                    jsonify(
                        {
                            "error": "A??o inv??lido",
                            "message": "El a??o debe estar entre 2000 y 2100",
                            "recibido": year,
                        }
                    ),
                    400,
                ),
            )

        return month, year, None

    except ValueError:
        return (
            None,
            None,
            (
                jsonify(
                    {
                        "error": "Formato de par??metros inv??lido",
                        "message": 'Los par??metros "month" y "year" deben ser n??meros enteros',
                        "recibido": {
                            "month": request.args.get("month"),
                            "year": request.args.get("year"),
                        },
                    }
                ),
                400,
            ),
        )


# =======================================
# ENDPOINTS
# =======================================


@app.route("/", methods=["GET"])
def index():
    """
    Endpoint ra??z - Informaci??n de la API
    """
    return jsonify(
        {
            "servicio": "AquaSenseCloud API",
            "version": "1.0",
            "description": "API REST para consultar datos de temperatura del Mar Menor",
            "endpoints": {
                "/health": "Health check del servidor",
                "/maxdiff": "Diferencia de m??xima temperatura mensual vs mes anterior",
                "/sd": "M??xima desviaci??n est??ndar mensual",
                "/temp": "Temperatura media mensual",
                "/months": "Lista de meses con datos disponibles",
            },
            "uso": {
                "maxdiff": "GET /maxdiff?month=3&year=2017",
                "sd": "GET /sd?month=3&year=2017",
                "temp": "GET /temp?month=3&year=2017",
                "months": "GET /months",
            },
            "proyecto": "Infraestructura para la Computaci??n de Altas Prestaciones - UPCT",
        }
    ), 200


@app.route("/health", methods=["GET"])
def health_check():
    """
    Health check endpoint para Application Load Balancer.

    Verifica:
        - Servicio Flask funcionando
        - Conectividad con DynamoDB

    Returns:
        JSON con estado del servicio (200 OK si healthy, 503 si unhealthy)
    """
    try:
        # Verificar conectividad con DynamoDB
        table.table_status

        logger.info("??? Health check: OK")

        return jsonify(
            {
                "status": "healthy",
                "servicio": "AquaSenseCloud API",
                "tabla": table_name,
                "mensaje": "Servicio operativo",
            }
        ), 200

    except Exception as e:
        logger.error(f"??? Health check failed: {str(e)}")

        return jsonify({"status": "unhealthy", "error": str(e)}), 503


@app.route("/maxdiff", methods=["GET"])
def get_maxdiff():
    """
    Endpoint: /maxdiff?month=M&year=Y

    Retorna la diferencia de m??xima temperatura del mes especificado
    respecto al mes anterior.

    Ejemplo:
        GET /maxdiff?month=4&year=2017

        Respuesta:
        {
            "month": 4,
            "year": 2017,
            "maxdiff": 2.14,
            "max_temp": 19.47,
            "last_updated": "2024-11-07T10:30:00Z",
            "record_count": 3
        }
    """
    try:
        # Validar par??metros
        month, year, error_response = validate_parameters(request)
        if error_response:
            return error_response

        # Contruir clave de b??squeda
        month_year = f"{year}-{month:02d}"

        logger.info(f"???? Consultando maxdiff: {month_year}")

        # Consultar DynamoDB
        response = table.get_item(
            Key={"monthYear": month_year, "metric_type": "maxdiff"}
        )

        # Verificar si existe el registro
        if "Item" not in response:
            logger.warning(f"?????? No hay datos para {month_year}")
            return jsonify(
                {
                    "error": "Datos no encontrados",
                    "message": f"No hay datos disponibles para {month}/{year}",
                    "month": month,
                    "year": year,
                }
            ), 404

        # Preparar respuesta
        item = decimal_to_float(response["Item"])

        result = {
            "month": month,
            "year": year,
            "maxdiff": item["value"],
            "max_temp": item.get("max_temp"),
            "last_updated": item.get("last_updated"),
            "record_count": item.get("record_count"),
        }

        logger.info(f"??? Maxdiff {month_year}: {item['value']}")

        return jsonify(result), 200

    except Exception as e:
        logger.error(f"??? Error en /maxdiff: {str(e)}")
        return jsonify({"error": "Error interno del servidor", "message": str(e)}), 500


@app.route("/sd", methods=["GET"])
def get_sd():
    """
    Endpoint: /sd?month=3&year=2017

    Retorna la m??xima desviaci??n est??ndar de temperatura del mes especificado.

    Ejemplo:
        GET /sd?month=3&year=2017

        Respuesta:
        {
            "month": 3,
            "year": 2017,
            "sd": 0.6254,
            "last_updated": "2024-11-07T10:30:00Z",
            "record_count": 2
        }
    """
    try:
        # Validar par??metros
        month, year, error_response = validate_parameters(request)
        if error_response:
            return error_response

        # Contruir clave de b??squeda
        month_year = f"{year}-{month:02d}"

        logger.info(f"???? Consultando sd: {month_year}")

        # Consultar DynamoDB
        response = table.get_item(Key={"monthYear": month_year, "metric_type": "sd"})

        # Verificar si existe el registro
        if "Item" not in response:
            logger.warning(f"?????? No hay datos para {month_year}")
            return jsonify(
                {
                    "error": "Datos no encontrados",
                    "message": f"No hay datos disponibles para {month}/{year}",
                    "month": month,
                    "year": year,
                }
            ), 404

        # Preparar respuesta
        item = decimal_to_float(response["Item"])

        result = {
            "month": month,
            "year": year,
            "sd": item["value"],
            "last_updated": item.get("last_updated"),
            "record_count": item.get("record_count"),
        }

        logger.info(f"??? SD {month_year}: {item['value']}")

        return jsonify(result), 200

    except Exception as e:
        logger.error(f"??? Error en /sd: {str(e)}")
        return jsonify({"error": "Error interno del servidor", "message": str(e)}), 500


@app.route("/temp", methods=["GET"])
def get_temp():
    """
    Endpoint: /temp?month=M&year=Y

    Retorna la temperatura media anual del mes especificado.

    Ejemplo:
        GET /temp?month=3&year=2017

        Respuesta:
        {
            "month": 3,
            "year": 2017,
            "temp": 17.06,
            "max_temp": 17.33,
            "last_updated": "2024-11-07T10:30:00Z",
            "record_count": 2
        }
    """
    try:
        # Validar par??metros
        month, year, error_response = validate_parameters(request)
        if error_response:
            return error_response

        # Construir clave de b??squeda
        month_year = f"{year}-{month:02d}"

        logger.info(f"???? Consultando temp: {month_year}")

        # Consultar DynamoDB
        response = table.get_item(Key={"monthYear": month_year, "metric_type": "temp"})

        # Verificar si existe el registro
        if "Item" not in response:
            logger.warning(f"?????? No hay datos para {month_year}")
            return jsonify(
                {
                    "error": "Datos no encontrados",
                    "message": f"No hay datos disponibles para {month}/{year}",
                    "month": month,
                    "year": year,
                }
            ), 404

        # Preparar respuesta
        item = decimal_to_float(response["Item"])

        result = {
            "month": month,
            "year": year,
            "temp": item["value"],
            "max_temp": item.get("max_temp"),
            "last_updated": item.get("last_updated"),
            "record_count": item.get("record_count"),
        }

        logger.info(f"??? Temp {month_year}: {item['value']}")

        return jsonify(result), 200

    except Exception as e:
        logger.error(f"??? Error en /temp: {str(e)}")
        return jsonify({"error": "Error interno del servidor", "message": str(e)}), 500


@app.route("/months", methods=["GET"])
def get_available_months():
    """
    Endpoint: /months

    Lista todos los meses disponibles en la base de datos.
    ??til para que los analistas conozcan qu?? datos est??n disponibles.

    Respuesta:
    {
        "months": [
            {"month_year": "2017-03", "metrics": ["maxdiff", "sd", "temp"]},
            {"month_year": "2017-04", "metrics": ["maxdiff", "sd", "temp"]}
        ],
        "count": 2
    }
    """
    try:
        logger.info("???? Listando meses disponibles")

        # Escanear tabla (apropiado para datasets peque??os)
        response = table.scan()
        items = response.get("Items", [])

        # Agrupar por mes
        months_dict = {}
        for item in items:
            month_year = item["month_year"]
            metric_type = item["metric_type"]

            if month_year not in months_dict:
                months_dict[month_year] = {"month_year": month_year, "metrics": []}

            months_dict[month_year]["metrics"].append(metric_type)

        # Convertir a lista y ordenar
        months_list = sorted(months_dict.values(), key=lambda x: x["month_year"])

        logger.info("??? Total meses: {len(months_list)}")

        return jsonify({"months": months_list, "count": len(months_list)}), 200

    except Exception as e:
        logger.error(f"??? Error en /months: {str(e)}")
        return jsonify({"error": "Error interno del servidor", "message": str(e)}), 500


# =======================================
# MANEJO DE ERRORES GLOBALES
# =======================================


@app.errorhandler(404)
def not_found(error):
    """Manejo de error 404 - Endpoint no encontrado"""
    logger.warning(f"?????? Endpoint no encontrado: {request.path}")
    return jsonify(
        {
            "error": "Endpoint no encontrado",
            "message": f'El endpoint "{request.path}" no existe',
            "endpoints_disponibles": [
                "/",
                "/health",
                "/maxdiff",
                "/sd",
                "/temp",
                "/months",
            ],
        }
    ), 404


@app.errorhandler(500)
def internal_error(error):
    """Majeo de error 500 - Error interno"""
    logger.error(f"??? Error interno del servidor: {str(error)}")
    return jsonify(
        {
            "error": "Error interno del servidor",
            "message": "Ha ocurrido un error inesperado. Por favor, int??ntelo de nuevo.",
        }
    ), 500


# =======================================
# PUNTO DE ENTRADA
# =======================================

if __name__ == "__main__":
    """
    Punto de entrada para ejecuaci??n local o en producci??n.

    Variables de entorno:
        PORT: Puerto del servidor (default: 8080)
        DEBUG: Modo debug (default: False)
    """
    port = int(os.environ.get("PORT", 8080))
    debug = os.environ.get("DEBUG", "False").lower() == "true"

    logger.info("=" * 70)
    logger.info(f"???? Iniciando AquaSenseCloud API")
    logger.info(f"   Puerto: {port}")
    logger.info(f"   Debug: {debug}")
    logger.info(f"   DynamoDB Table: {table_name}")
    logger.info("=" * 70)

    app.run(host="0.0.0.0", port=port, debug=debug)

PYTHONEOF

# --- 3. CONSTRUCCI??N DE LA IMAGEN DOCKER ---
docker build -t aquasense-api .

# --- 4. EJECUCI??N DEL CONTENEDOR DOCKER ---
# Estas variables se inyectar??n desde CloudFormation a la EC2, y luego 
# se pasan al contenedor.
DDB_TABLE_NAME="${DYNAMODB_TABLE_NAME}" # Usar?? el valor del !Sub en CloudFormation
AWS_REGION="${AWS_REGION}"             # Usar?? el valor del !Sub en CloudFormation

# docker run -d \
#   --name aquasense-web \
#   -p 8080:8080 \
#   -e DYNAMODB_TABLE=$DDB_TABLE_NAME \
#   -e AWS_REGION=$AWS_REGION \
#   aquasense-api

export AWS_DEFAULT_REGION="us-east-1"
export REGION=$AWS_DEFAULT_REGION
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
# REGION=$(aws configure get region)
aws ecr create-repository --repository-name aquasense-api

aws ecr get-login-password --region $REGION \
  | docker login --username AWS --password-stdin $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com

docker tag aquasense-api:latest $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/aquasense-api:latest

docker push $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/aquasense-api:latest
