#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 20 21:47:08 2023

@author: Sara García Cabezalí 
"""

from main_functions import SparkContext_app_setup, process_logs, __init__rdd_mapper
from VARIABLES import INPUT_DIR_HDFS
import os, sys

# --------------------- Map transformations -----------------

def narrow_transformation_map(conf_parameters, filename, filename_desc):
    sc = SparkContext_app_setup(conf_parameters)
    applicationId = sc.applicationId
    fp_file_input = os.path.join(INPUT_DIR_HDFS, filename)
    rdd_base = sc.textFile(fp_file_input)
    rdd_mapped = rdd_base.map(lambda x: __init__rdd_mapper(x, filename_desc))
    print('-------------------------------------------------------------')
    print(f'Example of rdd line after map transformation: {rdd_mapped.take(1)}')
    print('-------------------------------------------------------------')
    sc.stop()
    process_logs(applicationId)
        
        
def narrow_transformation_filter(conf_parameters, filename, filename_desc):
    sc = SparkContext_app_setup(conf_parameters)
    fp_file_input = os.path.join(INPUT_DIR_HDFS, filename)
    rdd_base = sc.textFile(fp_file_input)
    rdd_mapped = rdd_base.map(lambda x: __init__rdd_mapper(x, filename_desc))
    rdd_filtered =
    print(rdd_filtered)
    return(rdd_base)
        
        
def narrow_transformation_union(conf_parameters, filename, filename_desc):
    sc = SparkContext_app_setup(conf_parameters)
    fp_file_input = os.path.join(INPUT_DIR_HDFS, filename)
    rdd_base = sc.textFile(fp_file_input)
    return(rdd_base)
    
    
def apply_narrow_transformations(conf_parameters, filename, filename_desc):
    narrow_transformation_map(conf_parameters, filename, filename_desc)
    #narrow_transformation_filter(conf_parameters, filename, filename_desc)
    #narrow_transformation_union(conf_parameters, filename, filename_desc)

if __name__ == "__main__":
    try:
        assert(len(sys.argv) == 4)
        apply_narrow_transformations(sys.argv[1], sys.argv[2], sys.argv[3])
    except AssertionError:
        print('')
        print('-------------------------------------------Error------------------------------------------')
        print('Ejecuta el siguiente comando:')
        print('')
        print('                 python3 narrow_transformations.py <spark_conf_parameters> <filename_entrada> <filename_entrada_desc>')
        print('')
        print('   1. <spark_conf_parameters> [array]: parámetros para la configuración del SparkSession:')
        print('          [app_name,driver_cores,driver_memory,executor_instances,executor_cores,executor_memory]')
        print('')
        print('   2. <filename_entrada> [str]: nombre del fichero (loc ../input directory) sobre el que aplicar la función')
        print('')
        print('   3. <filename_entrada_desc> [str]: nombre del fichero con las descripciones de los campos del fichero <filename_entrada>')
        print('')
        print('-----------------------------------------------------------------------------------------')    
    except:
        raise
    