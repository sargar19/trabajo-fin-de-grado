"""
@author: Sara García Cabezalí
TFG: Estudio del mecanismo de redistribución de datos shuffle en Spark
Universidad Complutense de Madrid
"""

from main_functions import SparkContext_app_setup, read_and_build_base_rdd, process_logs
from VARIABLES import INPUT_DIR_HDFS
import os, sys
    
# --------------------- Map transformations -----------------

def narrow_transformation_map(conf_parameters: str, filename: str, filename_desc: str):
    """
    Aplica una transformación narrow utilizando map para modificar cada línea 
    de la RDD basándose en un mapeador.

    Args:
    conf_parameters (str): Los parámetros de configuración para iniciar el SparkContext.
    filename (str): El nombre del archivo que contiene los datos que se deben transformar.
    filename_desc (str): El nombre del archivo que contiene la descripción de los datos.

    Returns:
    None: La función no devuelve nada pero imprime un ejemplo de línea después de la transformación map.
    """
    
    # Define un identificador de la aplicación como prefijo del App Name y y configura el SparkContext
    conf_parameters = conf_parameters.replace('[', '[map_')
    sc, applicationId = SparkContext_app_setup(conf_parameters)
    
    # Lee y construye el RDD base
    rdd_base = read_and_build_base_rdd(sc, filename, filename_desc)
    
    # Imprime un ejemplo del RDD resultante
    print('-------------------------------------------------------------')
    print(f'Ejemplo de una línea rdd tras la aplicación de la transformación map: {rdd_base.take(1)}')
    print('-------------------------------------------------------------')
    
    # Detiene el SparkContext y procesa los logs
    sc.stop()
    process_logs(applicationId)


def narrow_transformation_filter(conf_parameters: str, filename: str, filename_desc: str):
    """
    Aplica una transformación narrow utilizando filter para 
    seleccionar líneas específicas de la RDD basada en ciertas condiciones.

    Args:
    conf_parameters (str): Los parámetros de configuración para iniciar el SparkContext.
    filename (str): El nombre del archivo que contiene los datos que se deben transformar.
    filename_desc (str): El nombre del archivo que contiene la descripción de los datos.

    Returns:
    None: La función no devuelve nada pero imprime ejemplos de líneas después de la transformación filter.
    """
    # Define un identificador de la aplicación como prefijo del App Name y y configura el SparkContext
    conf_parameters = conf_parameters.replace('[', '[filter_')
    sc, applicationId = SparkContext_app_setup(conf_parameters)
    
    # Lee y construye el RDD base
    rdd_base = read_and_build_base_rdd(sc, filename, filename_desc)
    
    # Días (histórico) en los que la temperatura máxima > 40 grados = 106 farenheit
    rdd_filtered = rdd_base.filter(lambda x: int(x[1])>= 2013 and float(x[18])>= 104)
    print('-------------------------------------------------------------')
    print(f'{rdd_filtered.take(3)}')
    print('-------------------------------------------------------------')

    # Detiene el SparkContext y procesa los logs
    sc.stop()
    process_logs(applicationId)
        
def narrow_transformation_union(conf_parameters: str, filename: str, filename_desc: str):
    """
    Aplica una transformación narrow utilizando union para combinar dos 
    muestras de la RDD original en una nueva RDD.

    Args:
    conf_parameters (str): Los parámetros de configuración para iniciar el SparkContext.
    filename (str): El nombre del archivo que contiene los datos que se deben transformar.
    filename_desc (str): El nombre del archivo que contiene la descripción de los datos.

    Returns:
    None: La función no devuelve nada pero imprime ejemplos de líneas después de la transformación union.
    """
    # Define un identificador de la aplicación como prefijo del App Name y y configura el SparkContext
    conf_parameters = conf_parameters.replace('[', '[union_')
    sc, applicationId = SparkContext_app_setup(conf_parameters)
    
    # Lee y construye el RDD base
    rdd_base = read_and_build_base_rdd(sc, filename, filename_desc)
    tamaño = 0.5
    rdd_sample_1 = rdd_base.sample(withReplacement = False, fraction = float(tamaño))
    rdd_sample_2 = rdd_base.sample(withReplacement = False, fraction = float(tamaño))
    rdd_unioned = rdd_sample_1.union(rdd_sample_2)
    
    print('-------------------------------------------------------------')
    print(f'{rdd_unioned.take(3)}')
    print('-------------------------------------------------------------')

    # Detiene el SparkContext y procesa los logs
    sc.stop()
    process_logs(applicationId)

# --------------------- Main map transformations -----------------


def main(conf_parameters, filename, filename_desc):
    try:
        # Limpia las comillas de los parámetros de configuración
        conf_parameters = conf_parameters.replace("'", '').replace('"', '')
        
        # Si 'filename' contiene '.', se asume que es un archivo individual y se aplican las transformaciones wide directamente
        if '.' in filename:
            file = filename.split(os.sep)[-1]
            app_name = '"app_narrow_transf_' + file +'"'
            conf_parameters = conf_parameters.replace('[','[' + app_name + ',')
            
            # Aplicando las transformaciones wide a un archivo individual
            narrow_transformation_map(str(conf_parameters), filename, filename_desc)
            narrow_transformation_filter(str(conf_parameters), filename, filename_desc)
            narrow_transformation_union(str(conf_parameters), filename, filename_desc)
        else:
            # Si 'filename' no contiene '.', se asume que es un directorio y se obtiene una lista de todos los archivos en el directorio
            SAMPLE_FILES_DIR = os.path.join(INPUT_DIR_HDFS,filename)
            app_name = 'check files in HDFS'
            conf_parameters_tmp = conf_parameters.replace('[','[' + app_name + ',')
            with SparkContext_app_setup(conf_parameters_tmp) as sc:
                fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
                sample_files = fs.listStatus(sc._jvm.org.apache.hadoop.fs.Path(SAMPLE_FILES_DIR))
                sc.stop()
                
            # Iterando sobre cada archivo en el directorio y aplicando las transformaciones wide a cada uno
            for file in sample_files:
                print(f'Applying narrow transformations to filename {file}.')
                file = file.getPath().getName()
                app_name = 'app_narrow_transf_' + file
                filename_final = os.path.join(filename,file)
                conf_parameters_final = conf_parameters.replace('[','[' + app_name + ',')
                for parameter in conf_parameters_final:
                    parameter.replace("'", '')
                narrow_transformation_map(str(conf_parameters_final), filename_final, filename_desc)
                narrow_transformation_filter(str(conf_parameters_final), filename_final, filename_desc)
                narrow_transformation_union(str(conf_parameters_final), filename_final, filename_desc)
        
    # Captura y manejo de errores genéricos
    except:
        print('------------------------- Error ---------------------------')
        print(f'Unable to process narrow_transfromations for samples for file {filename}')
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
        print('       python3 narrow_transformations.py <spark_conf_parameters> <filename_entrada / files_directory> <filename_entrada_desc>')
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
    