"""
AquaSense API REST - Servidor Web para consultar datos del Mar Menor

Autor: Equipo AquaSenseCloud

Descripción:
    API REST que proporciona acceso a las estadísticas de temperatura
    del Mar Menor almacenadas en DynamoDB con estructura de tabla plana.
    
    La tabla DynamoDB utiliza una estructura simplificada donde cada registro
    mensual contiene todos los datos agregados (temperatura media, máxima 
    desviación, diferencia con mes anterior) en una única fila.

Endpoints:
    GET /            - Información general de la API
    GET /health      - Health check del servidor
    GET /maxdiff     - Diferencia de temperatura máxima mensual vs mes anterior
    GET /sd          - Máxima desviación estándar mensual
    GET /temp        - Temperatura media mensual
    GET /months      - Lista de todos los meses con datos disponibles

Estructura de Tabla DynamoDB (Flat Table):
    Partition Key: monthYear (String, formato "YYYY-MM")
    Atributos:
        - max_temp: Temperatura máxima del mes (Decimal)
        - max_sd: Máxima desviación estándar del mes (Decimal)
        - mean_temp: Temperatura media del mes (Decimal)
        - max_diff_temp: Diferencia con temperatura máxima mes anterior (Decimal)
        - mean_temp_count: Número de registros procesados (Number)
        - last_updated: Timestamp de última actualización (String ISO)

Requisitos:
    - Flask 3.0.0
    - boto3 1.34.0
    - Python 3.11+

Variables de Entorno:
    - PORT: Puerto del servidor (default: 8080)
    - DYNAMODB_TABLE: Nombre de la tabla DynamoDB (default: "proy-MarMenorData")
    - AWS_REGION: Región AWS (default: "us-east-1")
"""

from flask import Flask, request, jsonify
import boto3
import os
from decimal import Decimal
import logging

# =======================================
# CONFIGURACIÓN
# =======================================

# Configuración de logging para trazabilidad
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Inicialización de Flask
app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False  # Mantener orden de claves en respuestas JSON

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
    
    DynamoDB devuelve números como objetos Decimal, que no son serializables
    directamente a JSON. Esta función convierte recursivamente todos los
    Decimal a float en diccionarios, listas y valores individuales.
    
    Args:
        obj: Objeto que puede contener Decimals (dict, list, Decimal, o cualquier tipo)
    
    Returns:
        Objeto con todos los Decimals convertidos a float
        
    Ejemplo:
        >>> decimal_to_float({"temp": Decimal("17.5"), "count": Decimal("3")})
        {"temp": 17.5, "count": 3.0}
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
    Valida los parámetros month y year de la petición HTTP.
    
    Verifica que:
    - Los parámetros existan
    - Sean números enteros válidos
    - El mes esté en el rango [1-12]
    - El año esté en el rango [2000-2100]
    
    Args:
        request: Objeto request de Flask con los query parameters
    
    Returns:
        tuple: (month, year, error_response)
            - Si es válido: (int, int, None)
            - Si hay error: (None, None, (json_response, status_code))
    
    Ejemplo:
        month, year, error = validate_parameters(request)
        if error:
            return error
        # Continuar con month y year validados
    """
    try:
        month_str = request.args.get("month")
        year_str = request.args.get("year")

        # Verificar que ambos parámetros existan
        if not month_str or not year_str:
            return (
                None, 
                None, 
                (jsonify({
                    "error": "Parámetros faltantes", 
                    "message": 'Los parámetros "month" y "year" son obligatorios',
                    "ejemplo": "/temp?month=3&year=2017"
                }), 400)
            )

        # Convertir a enteros
        month = int(month_str)
        year = int(year_str)

        # Validar rangos
        if not (1 <= month <= 12) or not (2000 <= year <= 2100):
            return (
                None, 
                None, 
                (jsonify({
                    "error": "Parámetros fuera de rango",
                    "message": "El mes debe estar entre 1-12 y el año entre 2000-2100"
                }), 400)
            )

        return month, year, None
        
    except ValueError:
        return (
            None, 
            None, 
            (jsonify({
                "error": "Formato inválido",
                "message": "Los parámetros deben ser números enteros"
            }), 400)
        )

# =======================================
# ENDPOINTS
# =======================================

@app.route("/", methods=["GET"])
def index():
    """
    Endpoint raíz - Información general de la API.
    
    Proporciona información sobre los endpoints disponibles y cómo usarlos.
    Útil para descubrimiento de API y documentación inicial.
    
    Returns:
        JSON con información de la API y lista de endpoints (200 OK)
        
    Ejemplo:
        GET /
        
        Response:
        {
            "servicio": "AquaSenseCloud API",
            "version": "2.0 (Flat Table)",
            "endpoints": {...}
        }
    """
    return jsonify({
        "servicio": "AquaSenseCloud API",
        "version": "2.0 (Flat Table)",
        "descripcion": "API REST para consultar datos de temperatura del Mar Menor",
        "endpoints": {
            "/health": "Health check del servidor",
            "/maxdiff": "Diferencia temperatura máxima mensual vs mes anterior",
            "/sd": "Máxima desviación estándar mensual",
            "/temp": "Temperatura media mensual",
            "/months": "Lista de meses con datos disponibles"
        },
        "uso": {
            "maxdiff": "GET /maxdiff?month=3&year=2017",
            "sd": "GET /sd?month=3&year=2017",
            "temp": "GET /temp?month=3&year=2017",
            "months": "GET /months"
        },
        "proyecto": "Infraestructura para la Computación de Altas Prestaciones - UPCT"
    }), 200

@app.route("/health", methods=["GET"])
def health_check():
    """
    Health check endpoint para monitoreo y load balancer.
    
    Verifica que:
    - El servicio Flask esté funcionando
    - La conexión con DynamoDB esté operativa
    
    Este endpoint es usado por el Application Load Balancer para determinar
    si la instancia debe recibir tráfico.
    
    Returns:
        JSON con estado "healthy" (200 OK) si todo funciona correctamente
        JSON con estado "unhealthy" (503 Service Unavailable) si hay errores
        
    Ejemplo:
        GET /health
        
        Response (exitoso):
        {
            "status": "healthy",
            "tabla": "proy-MarMenorData"
        }
    """
    try:
        # Verificar conectividad con DynamoDB
        table.table_status
        
        return jsonify({
            "status": "healthy", 
            "tabla": table_name
        }), 200
        
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return jsonify({
            "status": "unhealthy", 
            "error": str(e)
        }), 503

@app.route("/maxdiff", methods=["GET"])
def get_maxdiff():
    """
    Endpoint: /maxdiff?month=M&year=Y
    
    Retorna la diferencia de temperatura máxima del mes especificado
    respecto a la temperatura máxima del mes anterior.
    
    Requisito del proyecto:
        "Diferencia de la máxima temperatura del mes respecto a la máxima
        temperatura del mes anterior"
    
    Tabla DynamoDB (estructura plana):
        Key: {"monthYear": "YYYY-MM"}
        Atributo: max_diff_temp (Decimal)
    
    Query Parameters:
        month (int): Mes a consultar (1-12)
        year (int): Año a consultar (2000-2100)
    
    Returns:
        JSON con la diferencia de temperatura (200 OK)
        JSON con error (404 Not Found si no hay datos)
        JSON con error (400 Bad Request si parámetros inválidos)
        JSON con error (500 Internal Server Error si falla la consulta)
    
    Ejemplo:
        GET /maxdiff?month=4&year=2017
        
        Response:
        {
            "month": 4,
            "year": 2017,
            "maxdiff": 2.14,
            "max_temp": 19.47,
            "last_updated": "2024-12-07T10:30:00Z"
        }
    """
    try:
        # Validar parámetros de entrada
        month, year, error = validate_parameters(request)
        if error: 
            return error

        # Construir clave de consulta en formato YYYY-MM
        month_year = f"{year}-{month:02d}"
        
        logger.info(f"Consultando maxdiff para: {month_year}")
        
        # Consultar DynamoDB (tabla plana - solo Partition Key)
        response = table.get_item(Key={"monthYear": month_year})

        # Verificar si existe el registro
        if "Item" not in response:
            logger.warning(f"No hay datos para {month_year}")
            return jsonify({
                "error": "Datos no encontrados",
                "message": f"No hay datos disponibles para {month}/{year}"
            }), 404

        # Convertir Decimals a float para JSON
        item = decimal_to_float(response["Item"])

        # Preparar respuesta con los datos del atributo max_diff_temp
        result = {
            "month": month,
            "year": year,
            "maxdiff": item.get("max_diff_temp"),  # Diferencia con mes anterior
            "max_temp": item.get("max_temp"),       # Temperatura máxima del mes
            "last_updated": item.get("last_updated")
        }
        
        logger.info(f"Maxdiff {month_year}: {result['maxdiff']}°C")
        
        return jsonify(result), 200

    except Exception as e:
        logger.error(f"Error en /maxdiff: {str(e)}")
        return jsonify({
            "error": "Internal Server Error",
            "message": str(e)
        }), 500

@app.route("/sd", methods=["GET"])
def get_sd():
    """
    Endpoint: /sd?month=M&year=Y
    
    Retorna la máxima desviación estándar de temperatura del mes especificado.
    
    Requisito del proyecto:
        "Máxima desviación del conjunto de datos obtenidos durante el mes"
    
    La desviación estándar indica la variabilidad de las temperaturas durante
    el mes. Una desviación alta sugiere cambios bruscos de temperatura.
    
    Tabla DynamoDB (estructura plana):
        Key: {"monthYear": "YYYY-MM"}
        Atributo: max_sd (Decimal)
    
    Query Parameters:
        month (int): Mes a consultar (1-12)
        year (int): Año a consultar (2000-2100)
    
    Returns:
        JSON con la desviación estándar (200 OK)
        JSON con error (404 Not Found si no hay datos)
        JSON con error (400 Bad Request si parámetros inválidos)
        JSON con error (500 Internal Server Error si falla la consulta)
    
    Ejemplo:
        GET /sd?month=3&year=2017
        
        Response:
        {
            "month": 3,
            "year": 2017,
            "sd": 0.6254,
            "last_updated": "2024-12-07T10:30:00Z"
        }
    """
    try:
        # Validar parámetros de entrada
        month, year, error = validate_parameters(request)
        if error: 
            return error

        # Construir clave de consulta
        month_year = f"{year}-{month:02d}"
        
        logger.info(f"Consultando sd para: {month_year}")
        
        # Consultar DynamoDB (tabla plana)
        response = table.get_item(Key={"monthYear": month_year})

        # Verificar si existe el registro
        if "Item" not in response:
            logger.warning(f"No hay datos para {month_year}")
            return jsonify({
                "error": "Datos no encontrados",
                "message": f"No hay datos disponibles para {month}/{year}"
            }), 404

        # Convertir Decimals a float
        item = decimal_to_float(response["Item"])

        # Preparar respuesta con el atributo max_sd
        result = {
            "month": month,
            "year": year,
            "sd": item.get("max_sd"),  # Máxima desviación estándar del mes
            "last_updated": item.get("last_updated")
        }
        
        logger.info(f"SD {month_year}: {result['sd']}")
        
        return jsonify(result), 200

    except Exception as e:
        logger.error(f"Error en /sd: {str(e)}")
        return jsonify({
            "error": "Internal Server Error",
            "message": str(e)
        }), 500

@app.route("/temp", methods=["GET"])
def get_temp():
    """
    Endpoint: /temp?month=M&year=Y
    
    Retorna la temperatura media del mes especificado.
    
    Requisito del proyecto:
        "Temperatura media de los datos del mes"
    
    La temperatura media se calcula como el promedio ponderado de todas
    las mediciones semanales del mes, combinando datos de múltiples archivos
    CSV si es necesario.
    
    Tabla DynamoDB (estructura plana):
        Key: {"monthYear": "YYYY-MM"}
        Atributo: mean_temp (Decimal) - temperatura media calculada
    
    Query Parameters:
        month (int): Mes a consultar (1-12)
        year (int): Año a consultar (2000-2100)
    
    Returns:
        JSON con la temperatura media (200 OK)
        JSON con error (404 Not Found si no hay datos)
        JSON con error (400 Bad Request si parámetros inválidos)
        JSON con error (500 Internal Server Error si falla la consulta)
    
    Ejemplo:
        GET /temp?month=3&year=2017
        
        Response:
        {
            "month": 3,
            "year": 2017,
            "temp": 17.06,
            "max_temp": 17.33,
            "last_updated": "2024-12-07T10:30:00Z"
        }
    """
    try:
        # Validar parámetros de entrada
        month, year, error = validate_parameters(request)
        if error: 
            return error

        # Construir clave de consulta
        month_year = f"{year}-{month:02d}"
        
        logger.info(f"Consultando temp para: {month_year}")
        
        # Consultar DynamoDB (tabla plana)
        response = table.get_item(Key={"monthYear": month_year})

        # Verificar si existe el registro
        if "Item" not in response:
            logger.warning(f"No hay datos para {month_year}")
            return jsonify({
                "error": "Datos no encontrados",
                "message": f"No hay datos disponibles para {month}/{year}"
            }), 404

        # Convertir Decimals a float
        item = decimal_to_float(response["Item"])

        # Preparar respuesta con el atributo mean_temp
        result = {
            "month": month,
            "year": year,
            "temp": item.get("mean_temp"),    # Temperatura media del mes
            "max_temp": item.get("max_temp"), # Temperatura máxima del mes
            "last_updated": item.get("last_updated")
        }
        
        logger.info(f"Temp {month_year}: {result['temp']}°C")
        
        return jsonify(result), 200

    except Exception as e:
        logger.error(f"Error en /temp: {str(e)}")
        return jsonify({
            "error": "Internal Server Error",
            "message": str(e)
        }), 500

@app.route("/months", methods=["GET"])
def get_available_months():
    """
    Endpoint: /months
    
    Lista todos los meses disponibles en la base de datos.
    
    Útil para que los analistas conozcan qué períodos tienen datos disponibles
    antes de hacer consultas específicas. Con la estructura de tabla plana,
    cada registro en DynamoDB representa un mes completo con todas sus métricas.
    
    Tabla DynamoDB (estructura plana):
        Cada fila tiene monthYear como Partition Key único
        No hay Sort Key, por lo que cada mes es una fila independiente
    
    Returns:
        JSON con lista de meses (200 OK)
        JSON con error (500 Internal Server Error si falla el escaneo)
    
    Nota:
        Usa table.scan() que es costoso en tablas grandes, pero es aceptable
        para este dataset (máximo ~100 meses). Para datasets mayores, considerar
        usar un índice GSI o una tabla auxiliar de índice.
    
    Ejemplo:
        GET /months
        
        Response:
        {
            "months": ["2017-03", "2017-04", "2017-05", ...],
            "count": 45
        }
    """
    try:
        logger.info("Listando meses disponibles")
        
        # Escanear tabla completa (solo proyectando monthYear para eficiencia)
        response = table.scan(ProjectionExpression="monthYear")
        items = response.get("Items", [])

        # Extraer lista de meses únicos y ordenar
        # Como monthYear es la Partition Key, cada valor es único automáticamente
        months_list = sorted([item["monthYear"] for item in items])

        logger.info(f"Total meses disponibles: {len(months_list)}")
        
        return jsonify({
            "months": months_list, 
            "count": len(months_list)
        }), 200

    except Exception as e:
        logger.error(f"Error en /months: {str(e)}")
        return jsonify({
            "error": "Internal Server Error",
            "message": str(e)
        }), 500

# =======================================
# PUNTO DE ENTRADA
# =======================================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
