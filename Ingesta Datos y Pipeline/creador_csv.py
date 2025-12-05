# Codigo que coge el fichero de temperaturas.csv elige una linea del csv al azar y  crea un
# nuevo csv con las 20 lineas siguientes a este indice.

###################
#   LIBRERÍAS
###################
import numpy as np
import os

#################
# CODE
###############

def get_next_count(filename='contador.txt'):
    '''Genera un contador en un fichero externo para poder
        crear n ficheros csv'''
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            value = int(f.read())
    else:
        value = 1
    with open(filename, 'w') as f:
        f.write(str(value + 1))
    return value

def make_csv_from_csv(name_csv='./Data/Temperatura.csv', seed=None, len_csv=20, output_dir='./Data/'):
    '''Crea un nuevo CSV con len_csv líneas aleatorias consecutivas del CSV original'''
    
    # Verificar que el archivo existe
    if not os.path.exists(name_csv):
        raise FileNotFoundError(f"No se encuentra el archivo: {name_csv}")
    
    # Crear directorio de salida si no existe
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    with open(name_csv, 'r', encoding='utf-8') as csv_file:    
        lines_csv = csv_file.readlines()
    
    # Verificar que hay suficientes líneas
    if len(lines_csv) <= len_csv + 1:
        raise ValueError(f"El CSV no tiene suficientes líneas. Tiene {len(lines_csv)}, necesita al menos {len_csv + 1}")
    
    if seed is not None:
        np.random.seed(seed)
    
    # Asegurar que hay suficiente espacio para len_csv líneas después del índice
    index = np.random.randint(1, len(lines_csv) - len_csv)
    
    file_number = get_next_count()
    output_filename = os.path.join(output_dir, f'temperatura_{file_number}.csv')
    
    with open(output_filename, 'w', encoding='utf-8') as new_csv:
        new_csv.write(lines_csv[0])  # encabezado
        for l in range(index, index + len_csv):
            new_csv.write(lines_csv[l])
    
    print(f"Archivo creado: {output_filename} (líneas {index} a {index + len_csv - 1})")
    return output_filename

def generate_multiple_csv(num_files=5, name_csv='./Data/Temperatura.csv', len_csv=20, output_dir='./Data/'):
    '''Genera múltiples archivos CSV en el directorio especificado'''
    created_files = []
    
    print(f"Generando {num_files} archivos CSV en {output_dir}...")
    
    for i in range(num_files):
        try:
            filename = make_csv_from_csv(name_csv=name_csv, len_csv=len_csv, output_dir=output_dir)
            created_files.append(filename)
        except Exception as e:
            print(f"Error al crear archivo {i+1}: {e}")
    
    print(f"\nTotal de archivos creados: {len(created_files)}")
    return created_files

if __name__ == "__main__":
    # Generar 5 archivos CSV por defecto
    generate_multiple_csv(num_files=20)
    
    # O puedes personalizar los parámetros:
    # generate_multiple_csv(num_files=10, len_csv=30, output_dir='./Data/')