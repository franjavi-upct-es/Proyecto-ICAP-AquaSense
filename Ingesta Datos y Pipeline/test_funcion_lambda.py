"""
Tests Unitarios LOCALES para la Función Lambda de AquaSenseCloud

Este módulo contiene tests que se ejecutan 100% en local sin conexión a AWS.
Prueba la lógica pura de las funciones sin servicios externos.

Tests incluidos:
    - Parsing de CSV
    - Validación de datos
    - Cálculo de métricas (simulado)
    - Detección de alarmas (lógica)
    - Manejo de duplicados
    - Casos edge

Para ejecutar:
    pytest test_funcion_lambda.py -v
    
    o
    
    python test_funcion_lambda.py
"""

import pytest
import json
import os
from decimal import Decimal
from datetime import datetime
from io import StringIO
from unittest.mock import Mock, patch, MagicMock
import sys

# Configurar path
sys.path.insert(0, os.path.dirname(__file__))

# Mock de variables de entorno ANTES de cualquier import
os.environ['DYNAMODB_TABLE'] = 'test-MarMenorData'
os.environ['SNS_TOPIC_ARN'] = 'arn:aws:sns:us-east-1:123456789012:test-alerts'
os.environ['DEVIATION_THRESHOLD'] = '0.5'
os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

# Mock de boto3 ANTES de importar funcion_lambda
sys.modules['boto3'] = MagicMock()

# Ahora sí, importar
from funcion_lambda import parse_csv_data

# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def sample_csv_content():
    """Contenido CSV de ejemplo para tests"""
    return """Fecha,Medias,Desviaciones
2017/03/22,16.78,0.287
2017/03/30,17.32,0.403
2017/04/05,18.50,0.625
2017/04/12,19.20,0.450"""

@pytest.fixture
def sample_csv_with_duplicates():
    """CSV con fechas duplicadas para test de sobrescritura"""
    return """Fecha,Medias,Desviaciones
2017/03/22,16.78,0.287
2017/03/30,17.32,0.403
2017/03/22,18.00,0.350"""

@pytest.fixture
def sample_csv_high_deviation():
    """CSV con desviaciones altas para test de alarmas"""
    return """Fecha,Medias,Desviaciones
2017/03/22,16.78,0.287
2017/03/30,17.32,0.650
2017/04/05,18.50,0.825"""

@pytest.fixture
def mock_dynamodb_storage():
    """Almacén en memoria para simular DynamoDB"""
    return {}

# ============================================================================
# TESTS DE PARSING
# ============================================================================

class TestParseCsvData:
    """Tests para la función parse_csv_data"""
    
    def test_parse_valid_csv(self, sample_csv_content):
        """Test: parseo correcto de CSV válido"""
        result = parse_csv_data(sample_csv_content)
        
        assert len(result) == 4
        assert result[0]['year'] == 2017
        assert result[0]['month'] == 3
        assert result[0]['media'] == 16.78
        assert result[0]['desviacion'] == 0.287
        assert isinstance(result[0]['fecha'], datetime)
        print("✓ Test parsing CSV válido: PASADO")
    
    def test_parse_csv_with_duplicates(self, sample_csv_with_duplicates):
        """Test: parsing de CSV con fechas duplicadas"""
        result = parse_csv_data(sample_csv_with_duplicates)
        
        # Debe parsear todos los registros (el filtrado se hace al almacenar)
        assert len(result) == 3
        
        # Verificar que ambas entradas para 2017/03/22 están presentes
        march_22_records = [r for r in result if r['fecha'].day == 22]
        assert len(march_22_records) == 2
        print("✓ Test parsing con duplicados: PASADO")
    
    def test_parse_empty_csv(self):
        """Test: manejo de CSV vacío"""
        csv_content = "Fecha,Medias,Desviaciones\n"
        
        with pytest.raises(Exception, match="No se pudieron parsear registros válidos"):
            parse_csv_data(csv_content)
        print("✓ Test CSV vacío: PASADO")
    
    def test_parse_csv_invalid_date(self):
        """Test: manejo de fechas inválidas"""
        csv_content = """Fecha,Medias,Desviaciones
invalid-date,16.78,0.287
2017/03/30,17.32,0.403"""
        
        result = parse_csv_data(csv_content)
        
        # Debe saltarse la línea inválida
        assert len(result) == 1
        assert result[0]['media'] == 17.32
        print("✓ Test fecha inválida: PASADO")
    
    def test_parse_csv_missing_columns(self):
        """Test: manejo de columnas faltantes"""
        csv_content = """Fecha,Medias
2017/03/22,16.78
2017/03/30,17.32"""
        
        with pytest.raises(Exception, match="No se pudieron parsear registros válidos"):
            parse_csv_data(csv_content)
        print("✓ Test columnas faltantes: PASADO")
    
    def test_parse_csv_invalid_numbers(self):
        """Test: manejo de valores numéricos inválidos"""
        csv_content = """Fecha,Medias,Desviaciones
2017/03/22,invalid,0.287
2017/03/30,17.32,0.403"""
        
        result = parse_csv_data(csv_content)
        
        # Debe saltarse la línea con valor inválido
        assert len(result) == 1
        assert result[0]['media'] == 17.32
        print("✓ Test valores numéricos inválidos: PASADO")

# ============================================================================
# TESTS DE LÓGICA DE DATOS
# ============================================================================

class TestDataLogic:
    """Tests de lógica pura de procesamiento de datos"""
    
    def test_group_data_by_month(self, sample_csv_content):
        """Test: agrupación de datos por mes"""
        from collections import defaultdict
        
        weekly_data = parse_csv_data(sample_csv_content)
        
        # Agrupar por mes
        monthly_groups = defaultdict(list)
        for record in weekly_data:
            monthYear = f"{record['year']}-{record['month']:02d}"
            monthly_groups[monthYear].append(record)
        
        assert len(monthly_groups) == 2  # Marzo y abril
        assert len(monthly_groups['2017-03']) == 2
        assert len(monthly_groups['2017-04']) == 2
        print("✓ Test agrupación por mes: PASADO")
    
    def test_calculate_monthly_averages(self, sample_csv_content):
        """Test: cálculo de medias mensuales"""
        from collections import defaultdict
        
        weekly_data = parse_csv_data(sample_csv_content)
        
        # Agrupar y calcular
        monthly_groups = defaultdict(list)
        for record in weekly_data:
            monthYear = f"{record['year']}-{record['month']:02d}"
            monthly_groups[monthYear].append(record)
        
        # Marzo
        march_temps = [r['media'] for r in monthly_groups['2017-03']]
        march_avg = sum(march_temps) / len(march_temps)
        assert round(march_avg, 2) == 17.05  # (16.78 + 17.32) / 2
        
        # Abril
        april_temps = [r['media'] for r in monthly_groups['2017-04']]
        april_avg = sum(april_temps) / len(april_temps)
        assert round(april_avg, 2) == 18.85  # (18.50 + 19.20) / 2
        print("✓ Test cálculo de medias: PASADO")
    
    def test_duplicate_overwrite_logic(self):
        """Test: lógica de sobrescritura de duplicados"""
        # Simular almacenamiento
        storage = {}
        
        # Primera carga
        data1 = parse_csv_data("""Fecha,Medias,Desviaciones
2017/12/04,10.0,0.2
2017/12/10,20.0,0.3
2017/12/19,15.0,0.25""")
        
        for record in data1:
            key = record['fecha'].strftime('%Y-%m-%d')
            storage[key] = record['media']
        
        assert storage['2017-12-04'] == 10.0
        
        # Segunda carga con duplicado
        data2 = parse_csv_data("""Fecha,Medias,Desviaciones
2017/12/04,15.0,0.22
2017/12/15,20.0,0.28
2017/12/27,10.0,0.30""")
        
        for record in data2:
            key = record['fecha'].strftime('%Y-%m-%d')
            storage[key] = record['media']  # Sobrescribe
        
        # Verificar sobrescritura
        assert storage['2017-12-04'] == 15.0, "Debe sobrescribir con nuevo valor"
        assert len(storage) == 5, "Debe haber 5 registros únicos"
        
        # Calcular media correcta
        avg = sum(storage.values()) / len(storage)
        assert avg == 16.0, "Media debe ser 16.0°C"
        print("✓ Test lógica de duplicados: PASADO")
    
    def test_max_temperature_calculation(self, sample_csv_content):
        """Test: cálculo de temperatura máxima"""
        weekly_data = parse_csv_data(sample_csv_content)
        
        march_data = [r for r in weekly_data if r['month'] == 3]
        april_data = [r for r in weekly_data if r['month'] == 4]
        
        march_max = max(r['media'] for r in march_data)
        april_max = max(r['media'] for r in april_data)
        
        assert march_max == 17.32
        assert april_max == 19.20
        print("✓ Test temperatura máxima: PASADO")
    
    def test_max_deviation_calculation(self, sample_csv_content):
        """Test: cálculo de desviación máxima"""
        weekly_data = parse_csv_data(sample_csv_content)
        
        march_data = [r for r in weekly_data if r['month'] == 3]
        
        march_max_dev = max(r['desviacion'] for r in march_data)
        
        assert march_max_dev == 0.403
        print("✓ Test desviación máxima: PASADO")

# ============================================================================
# TESTS DE LÓGICA DE ALARMAS
# ============================================================================

class TestDeviationAlertsLogic:
    """Tests de lógica de alarmas sin SNS"""
    
    def test_identify_records_above_threshold(self, sample_csv_high_deviation):
        """Test: identificar registros que superan el umbral"""
        weekly_data = parse_csv_data(sample_csv_high_deviation)
        
        # Lógica de detección
        threshold = 0.5
        alerts = [r for r in weekly_data if r['desviacion'] > threshold]
        
        assert len(alerts) == 2, "Debe haber 2 registros sobre el umbral"
        assert alerts[0]['desviacion'] == 0.650
        assert alerts[1]['desviacion'] == 0.825
        print("✓ Test identificación de alarmas: PASADO")
    
    def test_no_alerts_below_threshold(self, sample_csv_content):
        """Test: no hay alertas bajo el umbral"""
        weekly_data = parse_csv_data(sample_csv_content)
        
        threshold = 0.5
        alerts = [r for r in weekly_data if r['desviacion'] > threshold]
        
        # La última tiene 0.625 > 0.5
        assert len(alerts) == 1
        print("✓ Test sin alarmas bajo umbral: PASADO")
    
    def test_alert_data_structure(self, sample_csv_high_deviation):
        """Test: estructura de datos de alerta"""
        weekly_data = parse_csv_data(sample_csv_high_deviation)
        
        alert_record = [r for r in weekly_data if r['desviacion'] > 0.6][0]
        
        # Verificar que tiene todos los campos necesarios
        assert 'fecha' in alert_record
        assert 'desviacion' in alert_record
        assert 'media' in alert_record
        assert isinstance(alert_record['fecha'], datetime)
        assert isinstance(alert_record['desviacion'], float)
        print("✓ Test estructura de alerta: PASADO")
    
    def test_alert_message_content(self):
        """Test: contenido esperado del mensaje de alerta"""
        # Simular datos de alerta
        record = {
            'fecha': datetime(2017, 3, 30),
            'desviacion': 0.650,
            'media': 17.32
        }
        
        # Simular construcción de mensaje
        fecha_str = record['fecha'].strftime("%Y-%m-%d")
        mensaje = f"Alerta: {fecha_str} - Desviación: {record['desviacion']:.4f}°C"
        
        assert "2017-03-30" in mensaje
        assert "0.6500" in mensaje
        print("✓ Test contenido de mensaje: PASADO")

# ============================================================================
# TESTS DE CASOS EDGE
# ============================================================================

class TestEdgeCases:
    """Tests para casos límite y edge cases"""
    
    def test_parse_csv_with_whitespace(self):
        """Test: manejo de espacios en blanco en CSV"""
        csv_content = """Fecha,Medias,Desviaciones
  2017/03/22  ,  16.78  ,  0.287  
2017/03/30,17.32,0.403"""
        
        result = parse_csv_data(csv_content)
        
        assert len(result) == 2
        assert result[0]['media'] == 16.78
        print("✓ Test espacios en blanco: PASADO")
    
    def test_parse_csv_very_high_temperature(self):
        """Test: manejo de temperaturas extremas"""
        csv_content = """Fecha,Medias,Desviaciones
2017/03/22,99.99,5.0"""
        
        result = parse_csv_data(csv_content)
        
        assert len(result) == 1
        assert result[0]['media'] == 99.99
        print("✓ Test temperatura extrema: PASADO")
    
    def test_parse_csv_negative_temperature(self):
        """Test: manejo de temperaturas negativas"""
        csv_content = """Fecha,Medias,Desviaciones
2017/03/22,-5.5,0.5"""
        
        result = parse_csv_data(csv_content)
        
        assert len(result) == 1
        assert result[0]['media'] == -5.5
        print("✓ Test temperatura negativa: PASADO")
    
    def test_single_record(self):
        """Test: un único registro"""
        csv_content = """Fecha,Medias,Desviaciones
2017/03/22,16.78,0.287"""
        
        result = parse_csv_data(csv_content)
        
        assert len(result) == 1
        print("✓ Test registro único: PASADO")
    
    def test_multiple_months_same_year(self):
        """Test: múltiples meses en el mismo año"""
        csv_content = """Fecha,Medias,Desviaciones
2017/01/15,15.0,0.2
2017/06/15,22.0,0.3
2017/12/15,16.0,0.25"""
        
        result = parse_csv_data(csv_content)
        
        months = set(r['month'] for r in result)
        assert len(months) == 3
        assert months == {1, 6, 12}
        print("✓ Test múltiples meses: PASADO")

# ============================================================================
# TESTS DE VALIDACIÓN DE DATOS
# ============================================================================

class TestDataValidation:
    """Tests de validación de datos"""
    
    def test_date_format_yyyy_mm_dd(self):
        """Test: formato de fecha YYYY/MM/DD"""
        csv_content = """Fecha,Medias,Desviaciones
2017/03/22,16.78,0.287"""
        
        result = parse_csv_data(csv_content)
        
        assert result[0]['fecha'].year == 2017
        assert result[0]['fecha'].month == 3
        assert result[0]['fecha'].day == 22
        print("✓ Test formato de fecha: PASADO")
    
    def test_float_precision(self):
        """Test: precisión de números flotantes"""
        csv_content = """Fecha,Medias,Desviaciones
2017/03/22,16.123456789,0.987654321"""
        
        result = parse_csv_data(csv_content)
        
        assert abs(result[0]['media'] - 16.123456789) < 0.0000001
        assert abs(result[0]['desviacion'] - 0.987654321) < 0.0000001
        print("✓ Test precisión flotante: PASADO")
    
    def test_zero_values(self):
        """Test: manejo de valores cero"""
        csv_content = """Fecha,Medias,Desviaciones
2017/03/22,0.0,0.0"""
        
        result = parse_csv_data(csv_content)
        
        assert result[0]['media'] == 0.0
        assert result[0]['desviacion'] == 0.0
        print("✓ Test valores cero: PASADO")

# ============================================================================
# CONFIGURACIÓN DE PYTEST
# ============================================================================

if __name__ == '__main__':
    # Permitir ejecutar directamente con python
    import sys
    
    # Contar tests
    print("="*70)
    print("  Tests Unitarios LOCALES - AquaSenseCloud")
    print("="*70)
    print("\nEjecutando tests sin conexión a AWS...")
    print("Probando lógica pura de las funciones\n")
    
    exit_code = pytest.main([__file__, '-v', '--tb=short'])
    
    print("\n" + "="*70)
    if exit_code == 0:
        print("✓ Todos los tests pasaron correctamente")
    else:
        print("✗ Algunos tests fallaron")
    print("="*70)
    
    sys.exit(exit_code)
