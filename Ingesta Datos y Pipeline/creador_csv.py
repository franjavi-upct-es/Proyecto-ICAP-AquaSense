# Codigo que coge el fichero de temperaturas.csv elige una linea del csv al azar y  crea un
# nuevo csv con las 20 lineas siguientes a este indice.

###################
#   LIBRERÍAS
###################
import numpy as np
from itertools import count
#################
# CODE
###############

import numpy as np
import os

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

def make_csv_from_csv(name_csv='Temperatura.csv', seed=None, len_csv=20):
    with open(name_csv, 'r') as csv_file:    
        lines_csv = csv_file.readlines()
    if seed != None:
        np.random.seed(seed)
    index = np.random.randint(1, len(lines_csv), len_csv)
    
    file_number = get_next_count()
    with open(f'temperatura_{file_number}.csv', 'w') as new_csv:
        new_csv.write(lines_csv[0])  # encabezado
        for l in index:
            new_csv.write(lines_csv[l])
            
make_csv_from_csv()