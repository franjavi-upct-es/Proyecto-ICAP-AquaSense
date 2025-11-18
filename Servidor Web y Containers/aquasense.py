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
from boto3.dynamodb.conditions import Key
import os
from decimal import Decimal
import logging

# =======================================
# CONFIGURACIÓN
# =======================================

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Inicialización de Flask
app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False # Mantener el orden de las claves en el JSON

# Cliente DynamoDB
dynamodb = boto3.resource('dynamodb', region_name=os.environ.get('AWS_REGION', 'us-east-1'))
table_name = os.environ.get('DYNAMODB_TABLE', 'proy-MarMenorData')
table = dynamodb.Table(table_name)

logger.info(f"🚀 AquaSenseCloud API iniciada")
logger.info(f"📊 DynamoDB Table: {table_name}")

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
        month_str = request.args.get('month')
        year_str = request.args.get('year')

        # Verificar que existan
        if not month_str or not year_str:
            return None, None, (jsonify({
                'error': 'Parámetros faltantes',
                'message': 'Los parámetros "month" y "year" son obligatorios',
                'ejemplo': '/maxdiff?month=3&year=2017'
            }), 400)

        # Convertir a enteros
        month = int(month_str)
        year = int(year_str)

        # Validar rangos
        if not (1 <= month <= 12):
            return None, None, (jsonify({
                'error': 'Mes inválido',
                'message': 'El mes debe estar entre 1 y 12',
                'recibido': month
            }), 400)

        if not (2000 <= year <= 2100):
            return None, None, (jsonify({
                'error': 'Año inválido',
                'message': 'El año debe estar entre 2000 y 2100',
                'recibido': year
            }), 400)

        return month, year, None

    except ValueError:
        return None, None, (jsonify({
            'error': 'Formato de parámetros inválido',
            'message': 'Los parámetros "month" y "year" deben ser números enteros',
            'recibido': {
                'month': request.args.get('month'),
                'year': request.args.get('year')
            }
        }), 400)

# =======================================
# ENDPOINTS
# =======================================

@app.route('/', methods=['GET'])
def index():
    """
    Endpoint raíz - Información de la API
    """
    return jsonify({
        'servicio': 'AquaSenseCloud API',
        'version': '1.0',
        'description': 'API REST para consultar datos de temperatura del Mar Menor',
        'endpoints': {
            '/health': 'Health check del servidor',
            '/maxdiff': 'Diferencia de máxima temperatura mensual vs mes anterior',
            '/sd': 'Máxima desviación estándar mensual',
            '/temp': 'Temperatura media mensual',
            '/months': 'Lista de meses con datos disponibles'
        },
        'uso': {
            'maxdiff': 'GET /maxdiff?month=3&year=2017',
            'sd': 'GET /sd?month=3&year=2017',
            'temp': 'GET /temp?month=3&year=2017',
            'months': 'GET /months'
        },
        'proyecto': 'Infraestructura para la Computación de Altas Prestaciones - UPCT'
    }), 200

@app.route('/health', methods=['GET'])
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

        logger.info('✅ Health check: OK')

        return jsonify({
            'status': 'healthy',
            'servicio': 'AquaSenseCloud API',
            'tabla': table_name,
            'mensaje': 'Servicio operativo'
        }), 200

    except Exception as e:
        logger.error(f"❌ Health check failed: {str(e)}")

        return jsonify({
            'status': 'unhealthy',
            'error': str(e)
        }), 503

@app.route('/maxdiff', methods=['GET'])
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