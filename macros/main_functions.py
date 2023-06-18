#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jun  4 11:19:17 2023

@author: Sara García Cabezalí
"""

from pyspark import SparkContext, SparkConf
import logging, os
from VARIABLES import INPUT_DIR


def SparkContext_app_setup(conf_parameters):
    # driver_cores = [1]
    # driver_memory = ['600mb','1500mb','3g']
    # executor_instances = [1,2,8]
    # executor_cores = [2,3,4]
    # executor_memory = ['600mb','1500mb','3g', '6g']
        
    try:
        assert conf_parameters.startswith('[') and conf_parameters.endswith(']') and (',') in conf_parameters
        conf_parameters = conf_parameters.strip('][').split(',')
        assert type(conf_parameters == list) and len(conf_parameters) == 6
        app_name, driver_cores, driver_memory, executor_instances, executor_cores, executor_memory = conf_parameters
        conf = SparkConf().setMaster("spark://dana:7077").setAppName(app_name).\
                            setAll([('spark.driver.cores', driver_cores),\
                                    ('spark.driver.memory', driver_memory),\
                                    ('spark.executor.instances', executor_instances),\
                                    ('spark.executor.cores', executor_cores),\
                                    ('spark.executor.memory',executor_memory)])
        sc = SparkContext(conf = conf)
        sc.setLogLevel('ERROR')
        #sc.addPyFile("py.zip")
        print("--------------------------------------------------------------------------------------------------")
        print(f"Correctly set up SparkContext for App {app_name}")
        print(sc)
        print("--------------------------------------------------------------------------------------------------")
        return sc
    except AssertionError:
        print('')
        print('-------------------------------------------Error------------------------------------------')
        print('Argumento <spark_conf_parameters> introducido es erróneo.')
        print('')
        print('      <spark_conf_parameters> [array]: parámetros para la configuración del SparkSession:')
        print('   [app_name, driver_cores, driver_memory, executor_instances, executor_cores, executor_memory]')
        print('')
        print('-----------------------------------------------------------------------------------------')

        #To do: Añadir aqui al log el AssertionError
        
    except Exception as exception:
        print('-------------------------------------------Error------------------------------------------')
        print(f"Unable to set up SparkContext configuration. The following exception was raised: {exception}")
        print('')
        print('Please check Spark input configuration parameters <spark_conf_parameters>.')
        print('')
        print('-----------------------------------------------------------------------------------------')
        raise

        #To do: Añadir aqui el log del exception

def __init__logger(level, name, filename, logger_file_mode, formatter):
    #logger_level = logging.DEBUG
    #formatter = '%(asctime)s - (name)s - (funcName)s - (message)s - (levelname)s'
    #logger_file_mode = 'a' or 'w'
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    file_handler = logging.FileHandler(filename, mode = logger_file_mode)
    formatter = logging.Formatter(formatter)
    file_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    
    return logger

#def build_directory(directory):
#    if not os.path.exists()


def get_input_file_fields(desc_filename):
    fp_desc_input = os.path.join(INPUT_DIR, desc_filename)
    try:
        with open(fp_desc_input, 'r') as df_desc: 
            for line_n, line in enumerate(df_desc):
                line = df_desc.readline()
                if 'FIELD' in line and 'POSITION' in line and 'TYPE' in line and 'DESCRIPTION' in line:    
                    break
            data = df_desc.readlines()
            json_fields = {}
            for line in data:
                if len(json_fields.keys()) == 26:
                    break
                elif line.split() != [] and line.split(' ')[0] != '':
                    field_name = line.split()[0]
                    field_type = line.split()[2]
                    if str.upper(field_name) != field_name:
                        field_name += '_' + list(json_fields.values())[-1][0]
                    json_fields[line.split()[1]] = [field_name, field_type]
                else:
                    pass
    except FileNotFoundError:
        print(f'{desc_filename} not found in {INPUT_DIR}. Please, check its existance or real location.')
    except Exception as exception:
        raise exception
    return json_fields


def __init__rdd_mapper(line, desc_filename):
    json_fields = get_input_file_fields(desc_filename)
    rdd_line = []
    for key in json_fields:
        beg = int(key.split('-')[0])-1
        end = int(key.split('-')[1])
        #Convert fields to respective types
        field_type = json_fields[key][1]
        rdd_line = rdd_line + [int(line[beg:end]) if field_type == 'Int.' else \
                               (float(line[beg:end]) if field_type == 'Real' \
                                else line[beg:end])]
    return(tuple(rdd_line))
    