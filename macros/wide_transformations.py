"""
@author: Sara García Cabezalí
TFG: Estudio del mecanismo de redistribución de datos shuffle en Spark
Universidad Complutense de Madrid
"""

from main_functions import SparkContext_app_setup, read_and_build_base_rdd, process_logs
from VARIABLES import INPUT_DIR_HDFS
import os,sys

# --------------------- Wide transformations -----------------

def wide_transformation_reduceByKey(conf_parameters: str, filename: str, filename_desc: str):
    """
    Aplica una transformación wide utilizando reduceByKey en un RDD creado a partir de un archivo especificado.
    
    Args:
        conf_parameters (str): Parámetros de configuración para el SparkContext.
        filename (str): El nombre del archivo que contiene los datos a procesar.
        filename_desc (str): El nombre del archivo que contiene la descripción de los datos.
        
    Returns:
        None: La función no devuelve nada pero imprime un ejemplo del RDD resultante.
    """
    
    # Define un identificador de la aplicación como prefijo del App Name y y configura el SparkContext
    conf_parameters = conf_parameters.replace('[', '[reduceByKey_')
    sc, applicationId = SparkContext_app_setup(conf_parameters)
    
    # Lee y construye el RDD base
    rdd_base = read_and_build_base_rdd(sc, filename, filename_desc)
    
    # Limpia el RDD y aplica una transformación map
    rdd_clean = rdd_base.filter(lambda x: x[1] != 9999.9).map(lambda x: (x[0],x[18]))
    
    # Aplica una reducción por clave usando la función max
    rdd_reduce = rdd_clean.reduceByKey(max)
    
    # Imprime un ejemplo del RDD resultante
    print('-------------------------------------------------------------')
    print(f'Ejemplo: {rdd_reduce.take(1)}')
    print('-------------------------------------------------------------')
    
    # Detiene el SparkContext y procesa los logs
    sc.stop()
    process_logs(applicationId)
    
    
def wide_transformation_groupByKey(conf_parameters: str, filename: str, filename_desc: str):
    """
    Aplica una transformación wide utilizando groupByKey en un RDD creado a partir de un archivo especificado.
    
    Args:
        conf_parameters (str): Parámetros de configuración para el SparkContext.
        filename (str): El nombre del archivo que contiene los datos a procesar.
        filename_desc (str): El nombre del archivo que contiene la descripción de los datos.
        
    Returns:
        None: La función no devuelve nada pero imprime algunos ejemplos del RDD resultante.
    """
    
    # Define un identificador de la aplicación como prefijo del App Name y y configura el SparkContext
    conf_parameters = conf_parameters.replace('[', '[groupByKey_')
    sc, applicationId = SparkContext_app_setup(conf_parameters)
    
    # Lee y construye el RDD base
    rdd_base = read_and_build_base_rdd(sc, filename, filename_desc)
    
    # Limpia el RDD y aplica una transformación map
    rdd_clean = rdd_base.filter(lambda x: x[1] != 9999.9).map(lambda x: (x[0],x[18]))
    
    # Aplica una agrupación por clave usando la función max
    rdd_group = rdd_clean.groupByKey().mapValues(max)
    
    # Imprime dos ejemplos del RDD resultante
    print('-------------------------------------------------------------')
    print(f'Ejemplos: {rdd_group.take(2)}')
    print('-------------------------------------------------------------')
    
    # Detiene el SparkContext y procesa los logs
    sc.stop()
    process_logs(applicationId)
    
    
def wide_transformation_distinct(conf_parameters: str, filename: str, filename_desc: str):
    """
    Encuentra y cuenta los valores distintos en las claves del RDD creado a partir de un archivo especificado.
    
    Args:
        conf_parameters (str): Parámetros de configuración para el SparkContext.
        filename (str): El nombre del archivo que contiene los datos a procesar.
        filename_desc (str): El nombre del archivo que contiene la descripción de los datos.
        
    Returns:
        None: La función no devuelve nada pero imprime el número de claves distintas encontradas en el RDD.
    """
    
    # Define un identificador de la aplicación como prefijo del App Name y y configura el SparkContext
    conf_parameters = conf_parameters.replace('[', '[distinct_')
    sc, applicationId = SparkContext_app_setup(conf_parameters)
    
    # Lee y construye el RDD base
    rdd_base = read_and_build_base_rdd(sc, filename, filename_desc)
    
    # Aplica una redución por clave de valores distintos
    rdd_distinct = rdd_base.keys().distinct()
    
    # Imprime el número de observaciones distintas
    print('-------------------------------------------------------------')
    print(f'Número de estaciones distintas en el fichero : {rdd_distinct.count()}')
    print('-------------------------------------------------------------')
    
    # Detiene el SparkContext y procesa los logs
    sc.stop()
    process_logs(applicationId)
    

# --------------------- Main wide transformations -----------------

def main(conf_parameters: str, filename: str, filename_desc: str):
    """
    Función principal que orquesta las transformaciones wide (reduceByKey, groupByKey, distinct) en uno o varios archivos especificados.
    
    Args:
        conf_parameters (str): Parámetros de configuración para el SparkContext.
        filename (str): El nombre del archivo o directorio que contiene los datos a procesar.
        filename_desc (str): El nombre del archivo que contiene la descripción de los datos.
        
    Returns:
        None: La función no devuelve nada, pero aplica transformaciones wide a los RDDs e imprime los resultados o errores en la consola.
    """
    
    try:
        # Limpia las comillas de los parámetros de configuración
        conf_parameters = conf_parameters.replace("'", '').replace('"', '')
        
        # Si 'filename' contiene '.', se asume que es un archivo individual y se aplican las transformaciones wide directamente
        if '.' in filename:
            file = filename.split(os.sep)[-1]
            app_name = 'app_narrow_transf_' + file
            conf_parameters = conf_parameters.replace('[','[' + app_name + ',')
            
            # Aplicando las transformaciones wide a un archivo individual
            wide_transformation_reduceByKey(str(conf_parameters), filename, filename_desc)
            wide_transformation_groupByKey(str(conf_parameters), filename, filename_desc)
            wide_transformation_distinct(str(conf_parameters), filename, filename_desc)
        else:
            # Si 'filename' no contiene '.', se asume que es un directorio y se obtiene una lista de todos los archivos en el directorio
            SAMPLE_FILES_DIR = os.path.join(INPUT_DIR_HDFS, filename)
            app_name = 'check files in HDFS'
            conf_parameters_tmp = conf_parameters.replace('[','[' + app_name + ',')
            sc, applicationId = SparkContext_app_setup(conf_parameters_tmp)
            fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
            sample_files = fs.listStatus(sc._jvm.org.apache.hadoop.fs.Path(SAMPLE_FILES_DIR))
            sc.stop()
            
            # Iterando sobre cada archivo en el directorio y aplicando las transformaciones wide a cada uno
            for file in sample_files:
                print(f'Applying wide transformations to filename {file}.')
                file = file.getPath().getName()
                app_name = 'app_narrow_transf_' + file
                filename_final = os.path.join(filename, file)
                conf_parameters_final = conf_parameters.replace('[','[' + app_name + ',')
                for parameter in conf_parameters_final:
                    parameter.replace("'", '')
                
                wide_transformation_reduceByKey(str(conf_parameters_final), filename_final, filename_desc)
                wide_transformation_groupByKey(str(conf_parameters_final), filename_final, filename_desc)
                wide_transformation_distinct(str(conf_parameters_final), filename_final, filename_desc)
   
    # Captura y manejo de errores genéricos
    except:
        print('------------------------- Error ---------------------------')
        print(f'Unable to process wide_transformations for samples for file {filename}')
        raise


if __name__ == "__main__":
    try:
        assert(len(sys.argv) == 4)
        main(sys.argv[1], sys.argv[2], sys.argv[3])
        
    # Manejo de errores para argumentos incorrectos
    except AssertionError:
        print('')
        print('-------------------------------------------Error------------------------------------------')
        print('Ejecuta el siguiente comando:')
        print('')
        print('       python3 wide_transformations.py <spark_conf_parameters> <filename_entrada / files_directory> <filename_entrada_desc>')
        print('')
        print('   1. <spark_conf_parameters> [array]: parámetros para la configuración del SparkSession:')
        print('          [driver_cores,driver_memory,executor_instances,executor_cores,executor_memory]')
        print('')
        print('   2. <filename_entrada / files_directory> [str]: nombre del fichero (loc ../input directory) sobre el que aplicar la función')
        print('                                                  o directorio in hdfs /input directory con multiples ficheros')
        print('')
        print('   3. <filename_entrada_desc> [str]: nombre del fichero con las descripciones de los campos del fichero <filename_entrada>')
        print('')
        print('-----------------------------------------------------------------------------------------')    
    except:
        raise
    