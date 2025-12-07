"""
AquaSense API REST - Servidor Web para consultar datos del Mar Menor

Autor: Equipo AquaSenseCloud

Descripción:
    API REST que proporciona acceso a las estadísticas de temperatura
    del Mar Menor almacenadas en DynamoDB

Endpoints:
    GET /health - Health check del servidor
    GET /maxdiff?month=M&year=Y - Diferencia de temperatura máxima mensual
    GET /sd?month=M&year=Y - Máxima desviación estándar mensual
    GET /temp?month=M&year=Y - Temperatura media mensual
    GET /months - Lista de todos los meses con datos disponibles

Requisitos:
    - Flask 3.0.0
    - boto3 1.34.0
    - Python 3.11+

Variables de Entorno:
    - PORT: Puerto del servidor (default: 8080)
    - DYNAMODB_TABLE: Nombre de la tabla DynamoDB
    - AWS_REGION: Región AWS (default: us-east-1)
"""

from flask import Flask, request, jsonify
import boto3
import os
from decimal import Decimal
import logging

# =======================================
# CONFIGURACIÓN
# =======================================

# Configuración de logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Inicialización de Flask
app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False  # Mantener el orden de las claves en el JSON

# Cliente DynamoDB
dynamodb = boto3.resource(
    "dynamodb", region_name=os.environ.get("AWS_REGION", "us-east-1")
)
table_name = os.environ.get("DYNAMODB_TABLE", "proy-MarMenorData")
table = dynamodb.Table(table_name)

logger.info(f"AquaSenseCloud API iniciada")
logger.info(f"DynamoDB Table: {table_name}")

# =======================================
# FUNCIONES AUXILIARES
# =======================================


def decimal_to_float(obj):
    """
    Convierte objetos Decimal de DynamoDB a float para serialización JSON.

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
    Valida los parámetros month y year de la request

    Args:
        request: Objeto request de Flask

    Returns:
        tuple: (month, year, error_response)
            - Si es válido: (int, int, None)
            - Si da error: (None, None, (json_response, status_code))
    """
    try:
        # Obtener parámetros
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
                            "error": "Parámetros faltantes",
                            "message": 'Los parámetros "month" y "year" son obligatorios',
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
                            "error": "Mes inválido",
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
                            "error": "Año inválido",
                            "message": "El año debe estar entre 2000 y 2100",
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
                        "error": "Formato de parámetros inválido",
                        "message": 'Los parámetros "month" y "year" deben ser números enteros',
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
    Endpoint raíz - Información de la API
    """
    return jsonify(
        {
            "servicio": "AquaSenseCloud API",
            "version": "1.0",
            "description": "API REST para consultar datos de temperatura del Mar Menor",
            "endpoints": {
                "/health": "Health check del servidor",
                "/maxdiff": "Diferencia de máxima temperatura mensual vs mes anterior",
                "/sd": "Máxima desviación estándar mensual",
                "/temp": "Temperatura media mensual",
                "/months": "Lista de meses con datos disponibles",
            },
            "uso": {
                "maxdiff": "GET /maxdiff?month=3&year=2017",
                "sd": "GET /sd?month=3&year=2017",
                "temp": "GET /temp?month=3&year=2017",
                "months": "GET /months",
            },
            "proyecto": "Infraestructura para la Computación de Altas Prestaciones - UPCT",
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

        logger.info("Health check: OK")

        return jsonify(
            {
                "status": "healthy",
                "servicio": "AquaSenseCloud API",
                "tabla": table_name,
                "mensaje": "Servicio operativo",
            }
        ), 200

    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")

        return jsonify({"status": "unhealthy", "error": str(e)}), 503


@app.route("/maxdiff", methods=["GET"])
def get_maxdiff():
    """
    Endpoint: /maxdiff?month=M&year=Y

    Retorna la diferencia de máxima temperatura del mes especificado
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
        # Validar parámetros
        month, year, error_response = validate_parameters(request)
        if error_response:
            return error_response

        # Contruir clave de búsqueda
        month_year = f"{year}-{month:02d}"

        logger.info(f"Consultando maxdiff: {month_year}")

        # Consultar DynamoDB
        response = table.get_item(
            Key={"monthYear": month_year, "metric_type": "maxdiff"}
        )

        # Verificar si existe el registro
        if "Item" not in response:
            logger.warning(f"No hay datos para {month_year}")
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

        logger.info(f"Maxdiff {month_year}: {item['value']}")

        return jsonify(result), 200

    except Exception as e:
        logger.error(f"Error en /maxdiff: {str(e)}")
        return jsonify({"error": "Error interno del servidor", "message": str(e)}), 500


@app.route("/sd", methods=["GET"])
def get_sd():
    """
    Endpoint: /sd?month=3&year=2017

    Retorna la máxima desviación estándar de temperatura del mes especificado.

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
        # Validar parámetros
        month, year, error_response = validate_parameters(request)
        if error_response:
            return error_response

        # Contruir clave de búsqueda
        month_year = f"{year}-{month:02d}"

        logger.info(f"📊 Consultando sd: {month_year}")

        # Consultar DynamoDB
        response = table.get_item(Key={"monthYear": month_year, "metric_type": "sd"})

        # Verificar si existe el registro
        if "Item" not in response:
            logger.warning(f"No hay datos para {month_year}")
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

        logger.info(f"SD {month_year}: {item['value']}")

        return jsonify(result), 200

    except Exception as e:
        logger.error(f"Error en /sd: {str(e)}")
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
        # Validar parámetros
        month, year, error_response = validate_parameters(request)
        if error_response:
            return error_response

        # Construir clave de búsqueda
        month_year = f"{year}-{month:02d}"

        logger.info(f"Consultando temp: {month_year}")

        # Consultar DynamoDB
        response = table.get_item(Key={"monthYear": month_year, "metric_type": "temp"})

        # Verificar si existe el registro
        if "Item" not in response:
            logger.warning(f"No hay datos para {month_year}")
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
        
        # Manejo de items legacy sin sum_temp
        # Si el item no tiene sum_temp, es un registro antiguo
        if 'sum_temp' not in item:
            logger.info(f"⚠️  Item legacy detectado para {month_year} (sin sum_temp)")
            # Para items legacy, usamos los valores tal como están
            result = {
                "month": month,
                "year": year,
                "temp": item["value"],
                "max_temp": item.get("max_temp"),
                "last_updated": item.get("last_updated"),
                "record_count": item.get("record_count", 1),
                "legacy": True  # Indicador de item legacy
            }
        else:
            # Item moderno con sum_temp
            result = {
                "month": month,
                "year": year,
                "temp": item["value"],
                "max_temp": item.get("max_temp"),
                "last_updated": item.get("last_updated"),
                "record_count": item.get("record_count"),
            }

        logger.info(f"Temp {month_year}: {item['value']}")

        return jsonify(result), 200

    except Exception as e:
        logger.error(f"Error en /temp: {str(e)}")
        return jsonify({"error": "Error interno del servidor", "message": str(e)}), 500


@app.route("/months", methods=["GET"])
def get_available_months():
    """
    Endpoint: /months

    Lista todos los meses disponibles en la base de datos.
    Útil para que los analistas conozcan qué datos están disponibles.

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
        logger.info("Listando meses disponibles")

        # Escanear tabla (apropiado para datasets pequeños)
        response = table.scan()
        items = response.get("Items", [])

        # Agrupar por mes
        months_dict = {}
        for item in items:
            month_year = item["monthYear"]
            metric_type = item["metric_type"]

            if month_year not in months_dict:
                months_dict[month_year] = {"month_year": month_year, "metrics": []}

            months_dict[month_year]["metrics"].append(metric_type)

        # Convertir a lista y ordenar
        months_list = sorted(months_dict.values(), key=lambda x: x["month_year"])

        logger.info("Total meses: {len(months_list)}")

        return jsonify({"months": months_list, "count": len(months_list)}), 200

    except Exception as e:
        logger.error(f"Error en /months: {str(e)}")
        return jsonify({"error": "Error interno del servidor", "message": str(e)}), 500


# =======================================
# MANEJO DE ERRORES GLOBALES
# =======================================


@app.errorhandler(404)
def not_found(error):
    """Manejo de error 404 - Endpoint no encontrado"""
    logger.warning(f"Endpoint no encontrado: {request.path}")
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
    logger.error(f"Error interno del servidor: {str(error)}")
    return jsonify(
        {
            "error": "Error interno del servidor",
            "message": "Ha ocurrido un error inesperado. Por favor, inténtelo de nuevo.",
        }
    ), 500


# =======================================
# PUNTO DE ENTRADA
# =======================================

if __name__ == "__main__":
    """
    Punto de entrada para ejecuación local o en producción.

    Variables de entorno:
        PORT: Puerto del servidor (default: 8080)
        DEBUG: Modo debug (default: False)
    """
    port = int(os.environ.get("PORT", 8080))
    debug = os.environ.get("DEBUG", "False").lower() == "true"

    logger.info("=" * 70)
    logger.info(f"Iniciando AquaSenseCloud API")
    logger.info(f"\tPuerto: {port}")
    logger.info(f"\tDebug: {debug}")
    logger.info(f"\tDynamoDB Table: {table_name}")
    logger.info("=" * 70)

    app.run(host="0.0.0.0", port=port, debug=debug)
