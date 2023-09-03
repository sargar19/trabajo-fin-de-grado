#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Aug 27 17:56:58 2023

@author: Sara García Cabezalí
"""

from main_functions import SparkContext_app_setup, process_logs, __init__rdd_mapper, get_input_file_fields
from VARIABLES import INPUT_DIR_HDFS
import os, sys


def groupByKey_var_partitions(n_partitions, conf_parameters, filename, filename_desc):
    sc = SparkContext_app_setup(conf_parameters)
    applicationId = sc.applicationId
    fp_file_input = os.path.join(INPUT_DIR_HDFS, filename)
    json_fields = get_input_file_fields(filename_desc)
    rdd_base = sc.textFile(fp_file_input)
    rdd_mapped = rdd_base.map(lambda x: __init__rdd_mapper(x, json_fields))
    rdd_mapped = rdd_mapped.map(lambda x: (x[0], 1))
    rdd_final = rdd_mapped.groupByKey().mapValues(sum)
    print('-------------------------------------------------------------')
    print(f'Número de estaciones distintas en el fichero : {rdd_final.count()}')
    print('-------------------------------------------------------------')
    sc.stop()
    process_logs(applicationId)


def main(conf_parameters, filename, filename_desc):
    try:
        conf_parameters = conf_parameters.replace("'", '').replace('"', '')
        if '.' in filename:
            file = filename.split(os.sep)[-1]
            for number_partitions in [1,1,2,2,3,3,4,4,5,5,6,6,7,7,8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23,24,24,25,25,50,50,100,100,200,200,300,300,400,400,500,500,600,600,700,700,800,800,900,900,1000,1000,1100,1100]:
                app_name = '"groupByKey_var_partitions_'+ str(number_partitions) + '_' + file +'"'
                conf_parameters_temp = conf_parameters.replace('[','[' + app_name + ',').replace(']',','+ str(number_partitions)+ ']')
                groupByKey_var_partitions(number_partitions, str(conf_parameters_temp), filename, filename_desc)
        else:
            SAMPLE_FILES_DIR = os.path.join(INPUT_DIR_HDFS,filename)
            app_name = 'check files in HDFS'
            conf_parameters_tmp = conf_parameters.replace('[','[' + app_name + ',')
            sc = SparkContext_app_setup(conf_parameters_tmp)
            fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
            sample_files = fs.listStatus(sc._jvm.org.apache.hadoop.fs.Path(SAMPLE_FILES_DIR))
            sc.stop()
            for file in sample_files:
                file = file.getPath().getName()
                filename_final = os.path.join(filename,file)
                for number_partitions in [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,50,100,200,300,400,500,600,700,800,900,1000,1100]:
                    app_name = 'groupByKey_var_partitions_' + str(number_partitions) + '_' + file
                    conf_parameters_final = conf_parameters.replace('[','[' + app_name + ',').replace(']',','+ str(number_partitions)+ ']')
                    for parameter in conf_parameters_final:
                        parameter.replace("'", '')
                    groupByKey_var_partitions(number_partitions, str(conf_parameters_final), filename_final, filename_desc)
    except:
        print('------------------------- Error ---------------------------')
        print(f'Unable to process Distinct transformation with varying shuffle partitions for file {filename}')
        raise

if __name__ == "__main__":
    try:
        assert(len(sys.argv) == 4)
        main(sys.argv[1], sys.argv[2], sys.argv[3])
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
    