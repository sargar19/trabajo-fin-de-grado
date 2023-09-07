"""
@author: Sara García Cabezalí
TFG: Estudio del mecanismo de redistribución de datos shuffle en Spark
Universidad Complutense de Madrid
"""

from main_functions import SparkContext_app_setup, read_and_build_base_rdd, process_logs
from VARIABLES import INPUT_DIR_HDFS
import os, sys


def GroupByKey_1(conf_parameters, filename, filename_desc):
    """
    Esta función realiza una transformación usando groupByKey para agrupar los datos por estación y contar el número 
    de registros para cada una.

    Args:
    conf_parameters (str): Parámetros de configuración para la inicialización del SparkContext.
    filename (str): El nombre del archivo que contiene los datos a procesar.
    filename_desc (str): El nombre del archivo que contiene la descripción de los datos.

    Returns:
    None: La función no retorna ningún valor, pero imprime el número de registros por estación en la consola.

    """
    
    # Define un identificador de la aplicación como prefijo del App Name y y configura el SparkContext
    conf_parameters = conf_parameters.replace('[', '[GroupByKey_1_')
    sc, applicationId = SparkContext_app_setup(conf_parameters)
    
    # Lee y construye el RDD base
    rdd_base = read_and_build_base_rdd(sc, filename, filename_desc)

    # Mapeo de los datos para tener pares (estación, 1) para cada registro
    rdd_mapped = rdd_base.map(lambda x: (x[0], 1))

    # Agrupando los datos por la clave (estación) y sumando los valores para obtener el conteo por estación
    rdd_group = rdd_mapped.groupByKey().mapValues(sum)

    print('-------------------------------------------------------------')
    print(f'El número de resgistros por estación es: {rdd_group.collect()}')
    print('-------------------------------------------------------------')
    
    # Detiene el SparkContext y procesa los logs
    sc.stop()
    process_logs(applicationId)


def ReduceByKey_1(conf_parameters, filename, filename_desc):
    """
    Esta función realiza una transformación usando reduceByKey para agrupar los datos por estación y contar el número 
    de registros para cada una.

    Args:
    conf_parameters (str): Parámetros de configuración para la inicialización del SparkContext.
    filename (str): El nombre del archivo que contiene los datos a procesar.
    filename_desc (str): El nombre del archivo que contiene la descripción de los datos.

    Returns:
    None: La función no retorna ningún valor, pero imprime el número de registros por estación en la consola.

    """
    
    # Define un identificador de la aplicación como prefijo del App Name y y configura el SparkContext
    conf_parameters = conf_parameters.replace('[', '[ReduceByKey_1_')
    sc, applicationId = SparkContext_app_setup(conf_parameters)
    
    # Lee y construye el RDD base
    rdd_base = read_and_build_base_rdd(sc, filename, filename_desc)

    # Mapeo de los datos para tener pares (estación, 1) para cada registro
    rdd_mapped = rdd_base.map(lambda x: (x[0], 1))

    # Reduciendo por clave (estación) para sumar los valores y obtener el conteo por estación
    rdd_reduce = rdd_mapped.reduceByKey(lambda a, b: a+b)

    print('-------------------------------------------------------------')
    print(f'El número de resgistros por estación es: {rdd_reduce.collect()}')
    print('-------------------------------------------------------------')
    
    # Detiene el SparkContext y procesa los logs
    sc.stop()
    process_logs(applicationId)


def GroupByKey_2(conf_parameters, filename, filename_desc):
    """
    Esta función realiza una transformación usando groupByKey para agrupar los datos por estación y encontrar la 
    temperatura mínima histórica para cada estación.

    Args:
    conf_parameters (str): Parámetros de configuración para la inicialización del SparkContext.
    filename (str): El nombre del archivo que contiene los datos a procesar.
    filename_desc (str): El nombre del archivo que contiene la descripción de los datos.

    Returns:
    None: La función no retorna ningún valor, pero imprime la temperatura mínima histórica por estación en la consola.

    """
    
    # Define un identificador de la aplicación como prefijo del App Name y y configura el SparkContext
    conf_parameters = conf_parameters.replace('[', '[GroupByKey_2_')
    sc, applicationId = SparkContext_app_setup(conf_parameters)
    
    # Lee y construye el RDD base
    rdd_base = read_and_build_base_rdd(sc, filename, filename_desc)

    # Mapeo de los datos para tener pares (estación, temperatura) para cada registro
    rdd_mapped = rdd_base.map(lambda x: (x[0], x[20]))

    # Agrupando los datos por la clave (estación) y encontrando el valor mínimo para obtener la temperatura mínima por estación
    rdd_group = rdd_mapped.groupByKey().mapValues(min)

    print('-------------------------------------------------------------')
    print(f'Temperatura mínima histórica por estación: {rdd_group.collect()}')
    print('-------------------------------------------------------------')
    
    # Detiene el SparkContext y procesa los logs
    sc.stop()
    process_logs(applicationId)




def ReduceByKey_2(conf_parameters: str, filename: str, filename_desc: str):
    """
    Esta función realiza una transformación utilizando `reduceByKey` para agrupar los datos por estación y encontrar la
    temperatura mínima histórica para cada una de ellas.

    Args:
    conf_parameters (str): Parámetros de configuración para la inicialización del SparkContext.
    filename (str): El nombre del archivo que contiene los datos a procesar.
    filename_desc (str): El nombre del archivo que contiene la descripción de los datos.

    Returns:
    None: La función no retorna ningún valor, pero imprime la temperatura mínima histórica por estación en la consola.

    """
    # Define un identificador de la aplicación como prefijo del App Name y y configura el SparkContext
    conf_parameters = conf_parameters.replace('[', '[ReduceByKey_2_')
    sc, applicationId = SparkContext_app_setup(conf_parameters)

    # Lee y construye el RDD base
    rdd_base = read_and_build_base_rdd(sc, filename, filename_desc)

    # Mapeo de los datos para tener pares (estación, temperatura) para cada registro
    rdd_mapped = rdd_base.map(lambda x: (x[0], x[20]))

    # Utilizando reduceByKey para encontrar la temperatura mínima histórica para cada estación
    rdd_reduce = rdd_mapped.reduceByKey(min)

    print('-------------------------------------------------------------')
    print(f'Temperatura mínima histórica por estación: {rdd_reduce.collect()}')
    print('-------------------------------------------------------------')

    # Detiene el SparkContext y procesa los logs
    sc.stop()
    process_logs(applicationId)


# --------------------- Main groupByKey and reduceByKey transformations -----------------

def main(conf_parameters, filename, filename_desc):
    try:
        # Limpia las comillas de los parámetros de configuración
        conf_parameters = conf_parameters.replace("'", '').replace('"', '')
        
        # Si 'filename' contiene '.', se asume que es un archivo individual y se aplican las transformaciones reduceByKey y groupByKey directamente
        if '.' in filename:
            file = filename.split(os.sep)[-1]
            app_name = '"app_narrow_transf_' + file +'"'
            conf_parameters = conf_parameters.replace('[','[' + app_name + ',')
            # Aplicando las transformaciones wide a un archivo individual
            GroupByKey_1(str(conf_parameters), filename, filename_desc)
            ReduceByKey_1(str(conf_parameters), filename, filename_desc)
            GroupByKey_2(str(conf_parameters), filename, filename_desc)
            ReduceByKey_2(str(conf_parameters), filename, filename_desc)
        else:
            # Si 'filename' no contiene '.', se asume que es un directorio y se obtiene una lista de todos los archivos en el directorio
            SAMPLE_FILES_DIR = os.path.join(INPUT_DIR_HDFS,filename)
            app_name = 'check files in HDFS'
            conf_parameters_tmp = conf_parameters.replace('[','[' + app_name + ',')
            sc, applicationId = SparkContext_app_setup(conf_parameters_tmp)
            fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
            sample_files = fs.listStatus(sc._jvm.org.apache.hadoop.fs.Path(SAMPLE_FILES_DIR))
            sc.stop()
            
            # Iterando sobre cada archivo en el directorio y aplicando las transformaciones reduceByKey y groupByKey a cada uno
            for file in sample_files:
                print(f'Applying narrow transformations to filename {file}.')
                file = file.getPath().getName()
                app_name = 'app_narrow_transf_' + file
                filename_final = os.path.join(filename,file)
                conf_parameters_final = conf_parameters.replace('[','[' + app_name + ',')
                for parameter in conf_parameters_final:
                    parameter.replace("'", '')
                GroupByKey_1(str(conf_parameters_final), filename_final, filename_desc)
                ReduceByKey_1(str(conf_parameters_final), filename_final, filename_desc)
                GroupByKey_2(str(conf_parameters_final), filename_final, filename_desc)
                ReduceByKey_2(str(conf_parameters_final), filename_final, filename_desc)
                
    # Captura y manejo de errores genéricos
    except:
        print('------------------------- Error ---------------------------')
        print(f'Unable to process GroupByKey and ReduceByKey transformations for file {filename}')
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
        print('       python3 groupByKeyvsreduceByKey.py <spark_conf_parameters> <filename_entrada / files_directory> <filename_entrada_desc>')
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
    