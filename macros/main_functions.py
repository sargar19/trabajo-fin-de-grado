"""
@author: Sara García Cabezalí
TFG: Estudio del mecanismo de redistribución de datos shuffle en Spark
Universidad Complutense de Madrid
"""

from pyspark import SparkContext, SparkConf
import os, subprocess, json, Tuple
import pandas as pd
from datetime import datetime
from tabulate import tabulate
from VARIABLES import INPUT_DIR, INPUT_DIR_HDFS, OUTPUT_DIR, LOG_DIR, LOG_DIR_SPARK, packages


def SparkContext_app_setup(conf_parameters: str) -> Tuple[SparkContext, str]:
    """
    Configura y crea un nuevo SparkContext utilizando los parámetros de configuración especificados.

    La cadena de parámetros de configuración debe estar en formato de lista JSON, conteniendo 
    entre 6 y 7 elementos, en el siguiente orden:
    - app_name (str): El nombre de la aplicación Spark.
    - driver_cores (str): El número de cores para el driver.
    - driver_memory (str): La cantidad de memoria para el driver.
    - executor_instances (str): El número de instancias ejecutoras.
    - executor_cores (str): El número de cores por instancia ejecutora.
    - executor_memory (str): La cantidad de memoria por instancia ejecutora.
    - n_partitions (str, opcional): El número de particiones por defecto para RDDs y DataFrames.

    Args:
        conf_parameters (str): La cadena de parámetros de configuración en formato de lista JSON.

    Returns:
        Tuple[SparkContext, str]: Una tupla conteniendo el SparkContext creado y el ID de la aplicación.

    Raises:
        AssertionError: Si los parámetros de configuración no están correctamente formateados o falta alguno.
        Exception: Si ocurre algún otro error durante la configuración del SparkContext.

    Ejemplo:
        conf_parameters = '["MyApp", "1", "600mb", "2", "2", "600mb", "200"]'
        sc, app_id = SparkContext_app_setup(conf_parameters)
    """
    try:
        # Asegurando que el argumento conf_parameters es una lista JSON y que tiene el número correcto de elementos
        assert conf_parameters.startswith('[') and conf_parameters.endswith(']') and (',') in conf_parameters
        conf_parameters = conf_parameters.strip('][').split(',')
        assert type(conf_parameters == list) and (len(conf_parameters) == 6 or len(conf_parameters) == 7)
        
        # Desempaquetando los valores de los parámetros de configuración
        if len(conf_parameters) == 6:
            app_name, driver_cores, driver_memory, executor_instances, executor_cores, executor_memory = conf_parameters
        else:
            app_name, driver_cores, driver_memory, executor_instances, executor_cores, executor_memory, n_partitions = conf_parameters
        
        # Creando una lista de tuplas con las configuraciones para Spark
        configurations = [
            ('spark.driver.cores', driver_cores),
            ('spark.driver.memory', driver_memory),
            ('spark.executor.instances', executor_instances),
            ('spark.executor.cores', executor_cores),
            ('spark.executor.memory',executor_memory),
            ('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
        ]
        if len(conf_parameters) == 7:
            configurations.append(('spark.default.parallelism', n_partitions))
        
        # Creando un objeto SparkConf y estableciendo todas las configuraciones
        conf = SparkConf().setMaster("spark://dana:7077").setAppName(app_name).\
                                setAll(configurations)
        
        # Creando un SparkContext con la configuración establecida y estableciendo el nivel de registro en 'ERROR'
        sc = SparkContext(conf = conf, pyFiles = packages + ['VARIABLES.py'])
        sc.setLogLevel('ERROR')
        
        # Obteniendo el ID de la aplicación Spark
        applicationId = sc.applicationId
        
        # Informando al usuario que el SparkContext se ha configurado correctamente
        print("--------------------------------------------------------------------------------------------------")
        print(f"Correctly set up SparkContext for App {app_name}")
        print("--------------------------------------------------------------------------------------------------")
        
        # Devolviendo el SparkContext y el ID de la aplicación
        return sc, applicationId
    
    except AssertionError:
        # Manejo de errores para argumentos incorrectos
        print('')
        print('-------------------------------------------Error------------------------------------------')
        print('Argumento <spark_conf_parameters> introducido es erróneo.')
        print('')
        print('      <spark_conf_parameters> [array]: parámetros para la configuración del SparkSession:')
        print('   [app_name, driver_cores, driver_memory, executor_instances, executor_cores, executor_memory]')
        print('')
        print('-----------------------------------------------------------------------------------------')
        
    except Exception as exception:
        # Manejo de otros errores
        print('-------------------------------------------Error------------------------------------------')
        print(f"Unable to set up SparkContext configuration. The following exception was raised: {exception}")
        print('')
        print('Please check Spark input configuration parameters <spark_conf_parameters>.')
        print('')
        print('-----------------------------------------------------------------------------------------')
        raise




def get_input_file_fields(desc_filename: str) -> dict:
    """
    Lee un archivo de descripción y extrae información sobre los campos contenidos en un archivo de entrada.

    Esta función espera que el archivo de descripción contenga líneas que definan campos con las palabras
    "FIELD", "POSITION", "TYPE" y "DESCRIPTION". Extrae y devuelve hasta 26 definiciones de campo en un
    formato de diccionario.

    Args:
        desc_filename (str): El nombre del archivo de descripción a leer.

    Returns:
        dict: Un diccionario donde las claves son las posiciones de los campos y los valores son listas
              que contienen el nombre y el tipo del campo correspondiente.

    Raises:
        FileNotFoundError: Si el archivo de descripción no se puede encontrar en el directorio INPUT_DIR.
        Exception: Para cualquier otra excepción que pueda ocurrir durante la ejecución.

    Example:
        desc_filename = "description.txt"
        fields = get_input_file_fields(desc_filename)
    """
    # Construyendo la ruta completa al archivo de descripción
    fp_desc_input = os.path.join(INPUT_DIR, desc_filename)
    
    try:
        # Abriendo el archivo de descripción para leer
        with open(fp_desc_input, 'r') as df_desc: 
            # Buscando la línea que contiene las palabras clave "FIELD", "POSITION", "TYPE", "DESCRIPTION"
            for line_n, line in enumerate(df_desc):
                line = df_desc.readline()
                if 'FIELD' in line and 'POSITION' in line and 'TYPE' in line and 'DESCRIPTION' in line:    
                    break
            # Leyendo todas las líneas restantes después de encontrar la línea con las palabras clave
            data = df_desc.readlines()
            # Inicializando el diccionario para almacenar información de los campos
            json_fields = {}
            # Iterando sobre cada línea de datos y extrayendo la información del campo
            for line in data:
                # Limitando el número de campos extraídos a 26
                if len(json_fields.keys()) == 26:
                    break
                # Procesando líneas no vacías que no comienzan con un espacio
                elif line.split() != [] and line.split(' ')[0] != '':
                    field_name = line.split()[0]
                    field_type = line.split()[2]
                    # Ajustando el nombre del campo si no está en mayúsculas
                    if str.upper(field_name) != field_name:
                        field_name += '_' + list(json_fields.values())[-1][0]
                    # Agregando la información del campo al diccionario
                    json_fields[line.split()[1]] = [field_name, field_type]
                else:
                    pass
        return json_fields

    # Manejo de excepciones
    except FileNotFoundError:
        print(f'{desc_filename} not found in {INPUT_DIR}. Please, check its existence or real location.')
        raise
    except Exception as exception:
        raise exception



def __init__rdd_mapper(line: str, json_fields: dict) -> list:
    """
    Mapea una línea de entrada según las especificaciones de campo proporcionadas.

    Esta función toma una línea de un archivo y un diccionario que especifica cómo deben extraerse y 
    convertirse los campos de esa línea. Los campos se extraen según los índices especificados y 
    se convierten al tipo de dato apropiado.

    Args:
        line (str): Una línea del archivo a procesar.
        json_fields (dict): Un diccionario con información sobre cómo extraer y convertir cada campo.

    Returns:
        list: Una lista de valores extraídos y convertidos de la línea de entrada.

    Example:
        line = "12345 1.23 abc"
        json_fields = {"0-5": ["int_field", "Int."], "6-10": ["float_field", "Real"], "11-14": ["str_field", "Str"]}
        mapped_line = __init__rdd_mapper(line, json_fields)
    """
    
    rdd_line = []
    # Iterando sobre cada campo especificado en json_fields
    for key in json_fields:
        # Determinando los índices de inicio y fin para extraer el campo de la línea
        beg = int(key.split('-')[0])-1
        end = int(key.split('-')[1])
        
        # Determinando el tipo de dato del campo
        field_type = json_fields[key][1]
        
        # Añadiendo el campo extraído y convertido a la lista
        # Si el tipo de campo es 'Int.', se convierte a int
        # Si el tipo de campo es 'Real', se convierte a float
        # De lo contrario, se deja como un string
        rdd_line = rdd_line + [int(line[beg:end]) if field_type == 'Int.' else 
                               (float(line[beg:end]) if field_type == 'Real' 
                                else line[beg:end])]
    return rdd_line



def read_and_build_base_rdd(sc, filename: str, filename_desc: str):
    """
    Lee un archivo de datos y construye un RDD base utilizando las especificaciones de un archivo de descripción.

    Esta función crea un RDD leyendo un archivo de datos de HDFS y luego mapea cada línea del archivo utilizando 
    la función `__init__rdd_mapper` que extrae y convierte los campos según lo especificado en un archivo de 
    descripción separado.

    Args:
        sc: El contexto de Spark.
        filename (str): El nombre del archivo de datos a leer.
        filename_desc (str): El nombre del archivo de descripción que contiene las especificaciones de campo.

    Returns:
        RDD: Un RDD donde cada elemento es una lista de campos extraídos y convertidos de una línea del archivo de datos.

    Example:
        sc = SparkContext(...)
        filename = "data.txt"
        filename_desc = "description.txt"
        rdd_base = read_and_build_base_rdd(sc, filename, filename_desc)
    """
    # Formando la ruta completa al archivo de datos en HDFS
    fp_file_input = os.path.join(INPUT_DIR_HDFS, filename)
    
    # Leyendo el archivo de datos y creando un RDD inicial
    rdd_base = sc.textFile(fp_file_input)
    
    # Obteniendo las especificaciones de campo del archivo de descripción
    json_fields = get_input_file_fields(filename_desc)
    
    # Mapeando cada línea del RDD usando las especificaciones de campo
    rdd_base = rdd_base.map(lambda x: __init__rdd_mapper(x, json_fields))
    
    return rdd_base



def move_event_logs(applicationId: str) -> bool:
    """
    Mueve los archivos de registro de eventos de una aplicación Spark a una nueva ubicación.

    Esta función utiliza el comando `mv` del sistema para mover los archivos de registro de una aplicación 
    específica de Spark del directorio de registro de Spark al directorio de registro local. 
    Si el movimiento es exitoso, retorna True; de lo contrario, retorna False.

    Args:
        applicationId (str): El ID de la aplicación Spark cuyos registros deben ser movidos.

    Returns:
        bool: True si el archivo de registro se movió exitosamente, de lo contrario False.

    Example:
        applicationId = "app-20230707215525-4415"
        was_successful = move_event_logs(applicationId)
    """
    
    # Definiendo las rutas de origen y destino para los archivos de registro
    fp_logfile_spark = os.path.join(LOG_DIR_SPARK, applicationId)
    fp_logfile = os.path.join(LOG_DIR, applicationId)
    
    # Intentando mover el archivo de registro usando el comando `mv`
    result = subprocess.run(['mv', fp_logfile_spark, fp_logfile])
    
    # Comprobando el código de retorno del comando para determinar si tuvo éxito
    if result.returncode == 1:
        print('----------------------------------------------------------------')
        print(f'Unable to move log file {applicationId} from spark log directory')
        print('----------------------------------------------------------------')
    else:
        print('----------------------------------------------------------------')
        print(f'Successfully moved log file {applicationId} to local directory')
        print('----------------------------------------------------------------')
    
    # Retornando el estado de éxito del movimiento
    return result.returncode == 0

        
def parse_logs(applicationId: str):
    """
    Parsea los logs de una aplicación de Spark y extrae información detallada del mismo, almacenándola en un DataFrame
    y, posteriormente, en un archivo CSV. 

    Args:
        applicationId (str): El ID de la aplicación de Spark cuyos logs se desean parsear.

    Returns:
        None. La función no devuelve ningún valor, pero genera un archivo CSV con la información extraída.
    """
    # Especifica el camino hacia el archivo log que será parseado.
    fp_logfile = os.path.join(OUTPUT_DIR,'logs', applicationId)
    
    # Inicializa un DataFrame para almacenar la información extraída de los logs.
    df = pd.DataFrame(columns = ['Datetime', 'Name', 'Job ID', 'Total Stages',\
                                 'Stage ID', 'Total Tasks', 'Task ID', 'Partition ID',\
                                 'SHUFFLER: Remote Blocks Fetched', 'SHUFFLER: Local Blocks Fetched',\
                                 'SHUFFLER: Fetch Wait Time', 'SHUFFLER: Remote Bytes Read',\
                                 'SHUFFLER: Remote Bytes Read To Disk',\
                                 'SHUFFLER: Local Bytes Read', 'SHUFFLER: Total Records Read',\
                                 'SHUFFLEW: Shuffle Bytes Written', 'SHUFFLEW: Shuffle Records Written',\
                                 'SHUFFLEW: Shuffle Write Time', 'Status'])
    try:
        # Abre el archivo log y lo carga como un JSON.
        with open(fp_logfile, 'r') as app_log_file:
            log_json = json.loads(json.dumps(app_log_file.readlines()))
            for line in log_json:
                line_json = json.loads(line)
                # Se categorizan los diferentes tipos de eventos que pueden ocurrir y se extrae la información relevante
                # de cada uno para almacenarla en el DataFrame
                
                #1. APPLICATION START LOGS
                if line_json['Event'] == 'SparkListenerApplicationStart':
                    start_time = datetime.fromtimestamp(int(line_json['Timestamp'])/1000)
                    app_name, app_id, user = str(line_json['App Name']), str(line_json['App ID']), str(line_json['User'])
                    df.loc[len(df)] = [start_time, 'APPLICATION START',"-", "-",\
                           "-", "-","-", "-","-", "-", "-", "-","-", "-",\
                           "-", "-", "-", "-", 'SUCCESS']
                    
                #2. APPLICATION END LOGS
                elif line_json['Event'] == 'SparkListenerApplicationEnd':
                    end_time = datetime.fromtimestamp(int(line_json['Timestamp'])/1000)
                    df.loc[len(df)] = [end_time, 'APPLICATION END', "-", "-", "-", "-","-", "-", "-", "-", "-", "-","-", "-", "-", "-", "-", "-", 'SUCCESS']
                    
                #3. JOB START LOGS
                elif line_json['Event'] == 'SparkListenerJobStart':
                    job_start_date_time = datetime.fromtimestamp(line_json['Submission Time']/1000)
                    job_id = line_json["Job ID"]
                    stage_ids = line_json["Stage IDs"]
                    n_stages = len(stage_ids)
                    df.loc[len(df)] = [job_start_date_time, 'JOB START', job_id, n_stages, "-", "-", "-", "-", "-", "-", "-", "-","-", "-","-", "-", "-", "-",'SUCCESS']
                    
                #4. JOB END LOGS
                elif line_json['Event'] == 'SparkListenerJobEnd':
                    job_end_date_time = datetime.fromtimestamp(line_json['Completion Time']/1000)
                    status = 'SUCCESS' if line_json['Job Result']['Result'] == 'JobSucceeded' else 'FAIL'
                    df.loc[len(df)] = [job_end_date_time, 'JOB END', job_id, n_stages, "-", "-", "-", "-", "-", "-", "-", "-","-", "-", "-", "-", "-", "-", status]
                    
                elif line_json['Event'] == 'SparkListenerStageSubmitted':
                    stage_start_date_time = datetime.fromtimestamp(line_json['Stage Info']['Submission Time']/1000)
                    stage_id = line_json['Stage Info']['Stage ID']
                    n_tasks = line_json['Stage Info']['Number of Tasks']
                    df.loc[len(df)] = [stage_start_date_time, 'STAGE START', job_id, n_stages, stage_id, n_tasks , "-", "-", "-", "-", "-", "-","-", "-", "-", "-", "-", "-", 'SUCCESS']
                    

                elif line_json['Event'] == 'SparkListenerStageCompleted':
                    stage_end_date_time = datetime.fromtimestamp(line_json['Stage Info']['Completion Time']/1000)
                    df.loc[len(df)] = [stage_end_date_time, 'STAGE END', job_id, n_stages, stage_id, n_tasks , "-", "-", "-", "-", "-", "-","-", "-", "-", "-", "-", "-", 'SUCCESS']
                    
                #5. TASK START LOGS
                elif line_json['Event'] == 'SparkListenerTaskStart':
                    task_start_date_time = datetime.fromtimestamp(line_json['Task Info']['Launch Time']/1000)
                    task_id = line_json['Task Info']['Task ID']
                    partition_id = str(line_json['Task Info']['Partition ID'])
                    status = 'FAIL' if line_json['Task Info']['Failed'] else 'SUCCESS'
                    df.loc[len(df)] = [task_start_date_time, 'TASK START', job_id, n_stages, stage_id, n_tasks , task_id, partition_id, "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", status]
                    
                #6. TASK END LOGS
                elif line_json['Event'] == 'SparkListenerTaskEnd':
                    task_end_date_time = datetime.fromtimestamp(line_json['Task Info']['Finish Time']/1000)
                    task_id = line_json['Task Info']['Task ID']
                    status = 'FAIL' if line_json['Task Info']['Failed'] else 'SUCCESS'
                    try:
                        shuffle_read_metrics = line_json['Task Metrics']['Shuffle Read Metrics']
                        rm_blocks_fetched = shuffle_read_metrics['Remote Blocks Fetched']
                        l_blocks_fetched = shuffle_read_metrics['Local Blocks Fetched']
                        fetch_wait_time = datetime.fromtimestamp(shuffle_read_metrics['Fetch Wait Time']/1000).second,
                        rm_bytes_read = shuffle_read_metrics['Remote Bytes Read']
                        rm_bytes_read_disk = shuffle_read_metrics['Remote Bytes Read To Disk']
                        l_bytes_read = shuffle_read_metrics['Local Bytes Read']
                        total_records_read = shuffle_read_metrics['Total Records Read']
                        try:
                            shuffle_write_metrics = line_json['Task Metrics']['Shuffle Write Metrics']
                            bytes_written = shuffle_write_metrics['Shuffle Bytes Written']
                            records_written = shuffle_write_metrics['Shuffle Records Written']
                            write_time = datetime.fromtimestamp(shuffle_write_metrics['Shuffle Write Time']/1000).second #Comprobar unidades"""
                        except KeyError:
                            shuffle_write_metrics = {'Shuffle Bytes Written': 'UNAVAILABLE','Shuffle Records Written': 'UNAVAILABLE',\
                                                'Shuffle Write Time':'UNAVAILABLE'}
                    except KeyError:
                        shuffle_read_metrics = {'Remote Blocks Fetched': 'UNAVAILABLE','Local Blocks Fetched': 'UNAVAILABLE',\
                                                'Fetch Wait Time':'UNAVAILABLE','Remote Bytes Read':'UNAVAILABLE',\
                                                'Remote Bytes Read To Disk':'UNAVAILABLE', 'Local Bytes Read': 'UNAVAILABLE',\
                                                'Total Records Read': 'UNAVAILABLE'}
                    print(f'================= TASK {task_id} ==================')
                    print('-------------- SHUFFLE READ --------------')
                    for key in shuffle_read_metrics:
                        print(key,shuffle_read_metrics[key])
                    print('-------------- SHUFFLE WRITE --------------')
                    for key in shuffle_write_metrics:
                        print(key,shuffle_write_metrics[key])
                    df.loc[len(df)] = [task_end_date_time, 'TASK END', job_id, \
                           n_stages, stage_id, n_tasks , task_id, partition_id,\
                           rm_blocks_fetched, l_blocks_fetched, fetch_wait_time,
                           rm_bytes_read, rm_bytes_read_disk, l_bytes_read,
                           total_records_read, bytes_written, records_written,\
                           write_time, status]
                    
        # Una vez que toda la información ha sido extraída y almacenada en el DataFrame, se añaden tres columnas adicionales 
        # para el nombre de la aplicación, el ID de la aplicación y el usuario.
        df[['App Name', 'App ID', 'User']] = [app_name,app_id,user]
        
        # Reordena las columnas del DataFrame y lo ordena por diferentes campos.
        first_columns = ['App ID', 'App Name', 'User', 'Datetime', 'Name', 'Job ID',\
                        'Total Stages', 'Stage ID', 'Total Tasks', 'Task ID', 'Partition ID', 'Status']
        rest_columns = [column for column in df.columns if column not in first_columns]
        df = df[first_columns + rest_columns]
        df.astype({'Job ID' : str, 'Stage ID' : str, 'Task ID' : str, 'Partition ID' : str})
        df.sort_values(by = ['Datetime','Job ID', 'Stage ID', 'Task ID'], inplace = True)
        
        # Guarda el DataFrame como un archivo CSV.
        df.to_csv(fp_logfile + '.csv', sep = ';')
        
        # Imprime resúmenes de la aplicación y los trabajos/stages/tareas.
        print('==================================== SUMMARY ====================================')
        df_summary_1 = df.loc[df['Job ID'] == "-"]
        df_summary_1 = [[app_name, app_id, user, (end_time - start_time).total_seconds()]]
        print('')
        print('Application summary: ')
        print('')
        print(tabulate(df_summary_1,headers=['App Name','App ID', 'User', 'Execution time (s)'], tablefmt = 'rounded_outline'))
        df_summary_2 = df.drop_duplicates(['Job ID', 'Stage ID', 'Total Tasks'])[['Job ID', 'Stage ID', 'Total Tasks']]
        df_summary_2 = df_summary_2.loc[df_summary_2['Job ID'] != "-"].loc[df_summary_2['Stage ID'] != "-"]
        print('')
        print('Jobs, Stages and Tasks summary: ')
        print('')
        print(tabulate(df_summary_2,showindex=False, headers = ['Job ID', 'Stage ID', 'Total Tasks'], tablefmt = 'rounded_outline'))
        print('==================================================================================')
        
    # Maneja la excepción en caso de que el archivo log no se encuentre en la ubicación especificada.
    except FileNotFoundError:
        print('--------------------ERROR------------------')
        print('Log file not found in logs directory')
        print('-------------------------------------------')
        print('TraceBackError:')
        raise


def process_logs(applicationId: str):
    """
    Esta función procesa los logs basados en el ID de una aplicación específica. Primero mueve
    los logs de eventos utilizando la función 'move_event_logs', y luego los parsea utilizando
    la función 'parse_logs'. 

    Args:
        applicationId (str): El ID de la aplicación para la que se procesarán los logs.

    Returns:
        None: La función no devuelve nada, pero como efecto secundario, los logs de eventos
        son movidos y parseados.

    Raises:
        Exception: Si ocurre un error durante la ejecución de 'move_event_logs' o 'parse_logs'.
    """
    try:
        # Intenta mover los logs de eventos para el applicationId especificado
        assert move_event_logs(applicationId)
        
        # Si el movimiento de los logs fue exitoso, procede a parsearlos
        parse_logs(applicationId)
    except Exception:
        # Si ocurre algún error durante el movimiento o el parseo de los logs, se informa al usuario y se relanza la excepción
        print('Unable to process and parse log file')
        raise
