"""
@author: Sara García Cabezalí
TFG: Estudio del mecanismo de redistribución de datos shuffle en Spark
Universidad Complutense de Madrid
"""

import sys, os, subprocess
from VARIABLES import INPUT_DIR_HDFS
from main_functions import SparkContext_app_setup, process_logs
import numpy as np

    
def sample_fichero(conf_parameters, filename_entrada, tamaños, samples_number, header):
    """
    Función que toma un archivo de entrada y crea múltiples muestras de este, guardándolas en HDFS.
    
    Args:
    conf_parameters (str): Parámetros de configuración para el SparkContext.
    filename_entrada (str): El nombre del archivo de entrada (debe estar ubicado en INPUT_DIR_HDFS).
    tamaños (list): Una lista de fracciones (como float) que determina el tamaño de cada muestra.
    samples_number (int): El número de muestras que deben ser creadas para cada tamaño en 'tamaños'.
    header (bool): Un indicador de si el archivo de entrada contiene un encabezado que debe preservarse en las muestras.
    
    Returns:
    None: La función no devuelve nada pero crea nuevos archivos en HDFS y genera logs.
    """
    # Construyendo las rutas de archivos necesarios utilizando os.path.join y manipulaciones de string
    fp_filename_entrada = os.path.join(INPUT_DIR_HDFS, filename_entrada)
    OUTPUT_DIR = os.path.join(INPUT_DIR_HDFS, filename_entrada[:filename_entrada.find('.')] + '_samples')
    TMP_OUTPUT_DIR = os.path.join(INPUT_DIR_HDFS, 'tmp')
    filename_prefix = filename_entrada[:filename_entrada.find('.')]
    
    # Modificando los parámetros de configuración y configurando el SparkContext y el FileSystem de Hadoop
    conf_parameters = conf_parameters.replace('[', '[build_samples_hdfs')
    sc, applicationId = SparkContext_app_setup(conf_parameters)
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
    
    # Verificando si el directorio de salida ya existe; si no, crea uno
    list_status = fs.listStatus(sc._jvm.org.apache.hadoop.fs.Path(INPUT_DIR_HDFS))
    if filename_entrada[:filename_entrada.find('.')] + '_samples' in list_status:
        pass
    else:
        subprocess.run(['hdfs','dfs','-mkdir', OUTPUT_DIR])
    
    # Leyendo el archivo de entrada como un RDD
    rdd_data = sc.textFile(fp_filename_entrada)
    
    # Si header es True, separa el encabezado del resto de los datos
    if header:
        rdd_data = rdd_data.map(lambda x: (1, x))
        header_line = rdd_data.filter(lambda x: x[0] == 1).map(lambda x: x[1])
        rdd_data = rdd_data.filter(lambda x: x[0] != 1).map(lambda x: x[1])
    
    # Looping a través de cada número de muestra y tamaño para crear muestras
    for sample_number in range(int(samples_number)):
        for tamaño in tamaños:
            tamaño = np.round(tamaño,2) # Redondeando el tamaño a dos decimales
            filename_salida = filename_prefix + '_sample_' + str(tamaño).zfill(3).replace('.','') + '_' + str(sample_number+2) + '.txt'
            
            # Creando una muestra del RDD original
            rdd_sample = rdd_data.sample(withReplacement = False, fraction = float(tamaño))
            
            # Si header es True, añade el encabezado de vuelta a la muestra
            if header:
                rdd_sample = header_line.union(rdd_sample)
            
            # Guardando la muestra como un archivo de texto y obteniendo su nombre
            rdd_sample.repartition(1).saveAsTextFile(TMP_OUTPUT_DIR)
            list_status = fs.listStatus(sc._jvm.org.apache.hadoop.fs.Path(TMP_OUTPUT_DIR))
            
            # Log del proceso de construcción de muestras
            print("=======================================")
            for file in list_status:
                filename_spark = file.getPath().getName()
                if filename_spark.startswith('part-'):
                    print(f'Sample file of size {float(tamaño)*100}% from original file {filename_entrada} built in directory {TMP_OUTPUT_DIR} under filename {filename_spark}')
                    break
            print("=======================================")
            
            # Mueve y renombra el archivo de muestra
            subprocess.run(['hdfs','dfs','-mv', os.path.join(TMP_OUTPUT_DIR, filename_spark), os.path.join(OUTPUT_DIR, filename_salida)])
            print(f'Sample file {filename_spark} renamed to desired filename: {filename_salida}')
            
            # Elimina el directorio temporal y su contenido
            fs.delete(sc._jvm.org.apache.hadoop.fs.Path(TMP_OUTPUT_DIR), True)
    
    # Detiene el SparkContext y procesa los logs
    sc.stop()
    process_logs(applicationId)


if __name__ == "__main__":
    try:
        assert len(sys.argv) == 6
        sample_fichero(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
        print(f'Creación del fichero de tamaño reducido {sys.argv[2]} satisfactoria.')   
        
    # Manejo de errores para argumentos incorrectos
    except AssertionError:
        print('------------------------------------- ERROR ----------------------------------------')
        print('Ejecuta el siguiente comando:')
        print('')
        print('    python3 build_sample_hdfs.py <spark_conf_parameters>  <filename_entrada> <filename_salida> <tamaño> <header>')
        print('')
        print('   1. <spark_conf_parameters> [array]: parámetros para la configuración del SparkSession:')
        print('          [driver_cores,driver_memory,executor_instances,executor_cores,executor_memory]')
        print('')
        print('   2. <filename> [str]: numbre del fichero para samplear (localizado en hdfs user/sargar19/input/)')
        print('')
        print('   3. <tamaños> [lst]: list de valores xi (float) entre 0 y 1 tales que size(sample_file) = xi * 100 % size(filename)')
        print('         Ejemplo: [0.01,0.1] --> construirá 2 samples con 1% y 10%, respectivamente, del tamaño del fichero original')
        print('')
        print('   4. <samples_number> [int]: número de ficheros samples de cada uno de los tamaños anteriores.')
        print('')
        print('   5. <header> [bool]: 0 si la primera linea de filename contiene nombres de las variables, 1 en caso contrario')
        print('')
        print('-----------------------------------------------------------------------------------')
    
    # Maneja la excepción en caso de que el archivo log no se encuentre en la ubicación especificada.
    except FileNotFoundError:
        print('------------------------------------- ERROR ----------------------------------------')
        print(f'No se encuentra el fichero {sys.argv[1]}. Por favor, comprueba la existencia de este.')
        print('------------------------------------------------------------------------------------')
        raise
    except Exception:
        print('')
        raise
