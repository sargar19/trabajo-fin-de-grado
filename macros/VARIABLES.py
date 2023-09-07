"""
@author: Sara García Cabezalí
TFG: Estudio del mecanismo de redistribución de datos shuffle en Spark
Universidad Complutense de Madrid
"""

import os

# DEFINICIÓN DE DIRECTORIOS (HDFS Y CLUSTER LOCAL)

#   1. DIRECTORIO LOCAL DEL CLUSTER
# Obtiene el directorio de trabajo actual
cd = os.getcwd()

# Define las rutas de los directorios de entrada, salida y logs para el cluster local
INPUT_DIR = '/home/sargar19/projects/tfg/input/'
OUTPUT_DIR = '/home/sargar19/projects/tfg/output'
LOG_DIR = os.path.join(OUTPUT_DIR, 'logs')

# Define el directorio donde Spark guarda los eventos (logs de Spark)
LOG_DIR_SPARK = os.path.join(os.sep.join(['/opt', 'spark', 'current', 'events']))

#   2. DIRECTORIOS HDFS
# Define las rutas de los directorios de entrada, salida y logs para el sistema de archivos HDFS
INPUT_DIR_HDFS = 'hdfs://dana:9000/user/sargar19/input/'
OUTPUT_DIR_HDFS = 'hdfs://dana:9000/user/sargar19/output/'
LOG_DIR_HDFS = os.path.join(OUTPUT_DIR_HDFS,'logs')


# -----------------------------------------

# Crear una lista de todos los archivos Python en el directorio de trabajo actual
packages = [file for file in os.listdir(cd) if file.endswith('.py')]
