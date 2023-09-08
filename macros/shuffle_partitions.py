"""
@author: Sara García Cabezalí
TFG: Estudio del mecanismo de redistribución de datos shuffle en Spark
Universidad Complutense de Madrid
"""

from main_functions import SparkContext_app_setup, read_and_build_base_rdd, process_logs
from VARIABLES import INPUT_DIR_HDFS
import os, sys


def groupByKey_var_partitions(conf_parameters: str, filename: str, filename_desc: str):
    """
    Esta función ejecuta una operación groupByKey en un RDD. 
    Luego cuenta y muestra el número de estaciones distintas presentes en el dataset.

    Args:
    conf_parameters (str): Parámetros de configuración para la inicialización del SparkContext.
    filename (str): El nombre del archivo que contiene los datos a procesar.
    filename_desc (str): El nombre del archivo que contiene la descripción de los datos.

    Returns:
    None: La función no retorna ningún valor, pero muestra el número de estaciones distintas en la consola.

    """
    # Iniciando el SparkContext con los parámetros de configuración proporcionados y construye RDD base
    sc, applicationId = SparkContext_app_setup(conf_parameters)
    rdd_base = read_and_build_base_rdd(sc, filename, filename_desc)
    
    # Mapeando cada registro en el RDD para extraer el identificador de la estación como la clave y asignar un valor de 1
    rdd_mapped = rdd_base.map(lambda x: (x[0], 1))
    
    # Agrupando los registros por la clave (identificador de la estación) y sumando los valores para obtener el recuento total por estación
    rdd_group = rdd_mapped.groupByKey().mapValues(sum)
    
    # Imprimiendo el número de estaciones distintas presentes en el dataset
    print('-------------------------------------------------------------')
    print(f'Número de estaciones distintas en el fichero : {rdd_group.count()}')
    print('-------------------------------------------------------------')
    
    # Detiene el SparkContext y procesa los logs
    sc.stop()
    process_logs(applicationId)


def main(conf_parameters: str, filename: str, filename_desc: str):
    """
    Esta función procesa archivos y aplica transformaciones Spark, con la posibilidad de variar las particiones de shuffle.

    Args:
        conf_parameters (str): Parámetros de configuración en formato string.
        filename (str): Nombre del archivo de entrada.
        filename_desc (str): Nombre del archivo de descripción.

    Raises:
        Exception: Se lanza una excepción en caso de error.

    Notas:
        - Esta función toma los parámetros de configuración y el nombre del archivo de entrada.
        - Si el nombre del archivo contiene un '.', se asume que es un archivo de datos, y se aplicarán
          transformaciones Spark con diferentes particiones.
        - Si no contiene un '.', se asume que es un archivo de descripción y se realizarán algunas operaciones
          adicionales.
        - En caso de error, se imprime un mensaje de error.

    """
    try:
        conf_parameters = conf_parameters.replace("'", '').replace('"', '')
        
        # Si 'filename' contiene '.', se asume que es un archivo individual y se aplican las transformaciones reduceByKey y groupByKey directamente
        if '.' in filename:
            file = filename.split(os.sep)[-1]

            # Iterar sobre un rango de particiones
            for number_partitions in range(1, 1000, 10):
                app_name = '"groupByKey_var_partitions_' + str(number_partitions) + '_' + file + '"'

                # Modificar los parámetros de configuración para incluir el nombre de la aplicación y las particiones
                conf_parameters_temp = conf_parameters.replace('[','[' + app_name + ',').replace(']',','+ str(number_partitions)+ ']')

                # Llamar a la función groupByKey_var_partitions con los parámetros modificados
                groupByKey_var_partitions(str(conf_parameters_temp), filename, filename_desc)
        
        # Si 'filename' no contiene '.', se asume que es un directorio y se obtiene una lista de todos los archivos en el directorio
        else:
            SAMPLE_FILES_DIR = os.path.join(INPUT_DIR_HDFS, filename)
            app_name = 'check files in HDFS'
            conf_parameters_tmp = conf_parameters.replace('[','[' + app_name + ',')
            sc, applicationId = SparkContext_app_setup(conf_parameters_tmp)
            fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
            sample_files = fs.listStatus(sc._jvm.org.apache.hadoop.fs.Path(SAMPLE_FILES_DIR))
            sc.stop()

            # Iterar sobre los archivos en el directorio
            for file in sample_files:
                file = file.getPath().getName()
                filename_final = os.path.join(filename, file)

                # Iterar sobre un rango de particiones y aplicar transformaciones
                for number_partitions in range(1, 1000, 10):
                    i = 0
                    while i < 3:
                        print(filename_final)
                        app_name = 'groupByKey_var_partitions_' + str(number_partitions) + '_' + file

                        # Modificar los parámetros de configuración para incluir el nombre de la aplicación y las particiones
                        conf_parameters_final = conf_parameters.replace('[','[' + app_name + ',').replace(']',','+ str(number_partitions)+ ']')
                        for parameter in conf_parameters_final:
                            parameter.replace("'", '')

                        # Llamar a la función groupByKey_var_partitions con los parámetros modificados
                        groupByKey_var_partitions(str(conf_parameters_final), filename_final, filename_desc)
                        i += 1
                        
    # Manejar excepciones e imprimir un mensaje de error
    except:
        print('------------------------- Error ---------------------------')
        print(f'Unable to process Distinct transformation with varying shuffle partitions for file {filename}')
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
    