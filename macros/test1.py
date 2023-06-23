#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 15 10:51:26 2023

@author: Sara García Cabezalí
"""

from main_functions import SparkContext_app_setup, __init__rdd_mapper
from VARIABLES import INPUT_DIR
from operator import add 
import os, sys

def count_distinct_keys(conf_parameters, filename, filename_desc):
    print('===================== APP START TIME ===========================')
    sc = SparkContext_app_setup(conf_parameters)
    print(f'Application ID: {sc.applicationId}')
    print(f'Spark App {sc.appName}')
    fp_file_input = os.path.join(INPUT_DIR, filename)
    print('============================BUILDING UP RDD_BASE============================')
    rdd_base = sc.textFile(fp_file_input)
    print(f'Example of the base rdd (lines of text file {filename}): {rdd_base.take(10)}')
    print(f"El número de particiones definidas as default: {rdd_base.getNumPartitions()}")
    rdd1 = rdd_base.map(lambda x: (x[1], 1))
    print(f'Example of the rdd keys:{rdd1.take(3)}')
    rdd1 = rdd1.reduceByKey(add)
    print(f'Ejemplo de número de instancias para cada clave: {rdd1.take(4)}')
    rdd = rdd_base.map(lambda x: __init__rdd_mapper(x,filename_desc))
    """
    print("============================================================================")
    print(rdd.take(3))
    print("============================================================================")"""
    print(rdd.take(2))
    print(f"Finished running Spark App {sc.appName}")
    print(f'Application ID: {sc.applicationId}')
    print(f'Check Spark UI for app browsing: http://192.168.134.1:18080/history/{sc.applicationId}')
    print(f'Check logs by typing command: yarn logs -applicationId {sc.applicationId.replace("app", "application").replace("-","_")}')
    sc.stop

if __name__ == "__main__":
    try:
        assert len(sys.argv) == 4
        count_distinct_keys(sys.argv[1], sys.argv[2], sys.argv[3])
        print(f'Creación del fichero de tamaño reducido {sys.argv[2]} satisfactoria.')   
    except AssertionError:
        print('')
        print('-------------------------------------------Error------------------------------------------')
        print('Ejecuta el siguiente comando:')
        print('')
        print('                 python3 test1.py <spark_conf_parameters> <filename_entrada> <filename_entrada_desc>')
        print('')
        print('   1. <spark_conf_parameters> [array]: parámetros para la configuración del SparkSession:')
        print('          [app_name,driver_cores,driver_memory,executor_instances,executor_cores,executor_memory]')
        print('')
        print('   2. <filename_entrada> [str]: nombre del fichero (loc ../input directory) sobre el que aplicar la función')
        print('')
        print('   3. <filename_entrada_desc> [str]: nombre del fichero con las descripciones de los campos del fichero <filename_entrada>')
        print('')
        print('-----------------------------------------------------------------------------------------')
    except Exception:
        print('')
        # Añadir al log la excepción
        raise

