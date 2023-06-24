#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 20 21:47:08 2023

@author: Sara García Cabezalí 
"""

from main_functions import rdd_mapper, SparkContext_app_setup
from VARIABLES import INPUT_DIR_HDFS
import os, sys

"""def simple_map(rdd_line):
    station = rdd_line[0]
    year = rdd_line[1]
    day= rdd_line[2]
    
    return(station, )"""
    

def narrow_transformation_map(conf_parameters, filename, filename_desc):
        sc = SparkContext_app_setup(conf_parameters)
        fp_file_input = os.path.join(INPUT_DIR_HDFS, filename)
        rdd_base = sc.textFile(fp_file_input)
        print(f'El número de registros del fichero original es:{rdd_base.count()}')
        rdd = rdd_base.map(lambda x: __init__rdd_mapper(x, filename_desc))
        print(rdd.take(1))
        sc.stop

def narrow_transformation_filter(conf_parameters, filename, filename_desc):
        sc = SparkContext_app_setup(conf_parameters)
        fp_file_input = os.path.join(INPUT_DIR_HDFS, filename)
        rdd_base = sc.textFile(fp_file_input)
        return(rdd_base)
        
def narrow_transformation_union(conf_parameters, filename, filename_desc):
        sc = SparkContext_app_setup(conf_parameters)
        fp_file_input = os.path.join(INPUT_DIR_HDFS, filename)
        rdd_base = sc.textFile(fp_file_input)
        return(rdd_base)
        

if __name__ == "__main__":
    try:
        assert(len(sys.argv) == 4)
        narrow_transformation_map(sys.argv[1], sys.argv[2], sys.argv[3])
        #narrow_transformation_flatMap(sys.argv[1], sys.argv[2], sys.argv[3])
        #narrow_transformation_filter(sys.argv[1], sys.argv[2], sys.argv[3])
        #narrow_transformation_union(sys.argv[1], sys.argv[2], sys.argv[3])
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
    