#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: Sara García Cabezalí
"""
from main_functions import SparkContext_app_setup, process_logs, get_input_file_fields, __init__rdd_mapper
from VARIABLES import INPUT_DIR_HDFS
import os,sys

# --------------------- Wide transformations -----------------


def wide_transformation_reduceByKey(conf_parameters, filename, filename_desc):
    conf_parameters = conf_parameters.replace('[', '[reduceByKey_')
    sc = SparkContext_app_setup(conf_parameters)
    applicationId = sc.applicationId
    fp_file_input = os.path.join(INPUT_DIR_HDFS, filename)
    json_fields = get_input_file_fields(filename_desc)
    rdd_base = sc.textFile(fp_file_input)
    rdd_mapped = rdd_base.map(lambda x: __init__rdd_mapper(x, json_fields))
    print('-------------------------------------------------------------')
    #Máxima (temperatura) histórica en cada estación
    rdd_final = rdd_mapped.filter(lambda x: x[1] != 9999.9).map(lambda x: (x[0],x[18]))
    rdd_final = rdd_final.reduceByKey(max)
    print('-------------------------------------------------------------')
    print(f'Example: {rdd_final.take(1)}')
    print('-------------------------------------------------------------')
    sc.stop()
    process_logs(applicationId)
    
def wide_transformation_groupByKey(conf_parameters, filename, filename_desc):
    conf_parameters = conf_parameters.replace('[', '[groupByKey_')
    sc = SparkContext_app_setup(conf_parameters)
    applicationId = sc.applicationId
    fp_file_input = os.path.join(INPUT_DIR_HDFS, filename)
    rdd_base = sc.textFile(fp_file_input)
    json_fields = get_input_file_fields(filename_desc)
    rdd_mapped = rdd_base.map(lambda x: __init__rdd_mapper(x, json_fields))
    print('-------------------------------------------------------------')
    #Máxima (temperatura) histórica en cada estación
    rdd_final = rdd_mapped.filter(lambda x: x[1] != 9999.9).map(lambda x: (x[0],x[18]))
    rdd_final = rdd_final.groupByKey().mapValues(max)
    print('-------------------------------------------------------------')
    print(f'Examples: {rdd_final.take(2)}')
    print('-------------------------------------------------------------')
    sc.stop()
    process_logs(applicationId)
    
def wide_transformation_distinct(conf_parameters, filename, filename_desc):
    conf_parameters = conf_parameters.replace('[', '[distinct_')
    sc = SparkContext_app_setup(conf_parameters)
    applicationId = sc.applicationId
    fp_file_input = os.path.join(INPUT_DIR_HDFS, filename)
    rdd_base = sc.textFile(fp_file_input)
    json_fields = get_input_file_fields(filename_desc)
    rdd_mapped = rdd_base.map(lambda x: __init__rdd_mapper(x, json_fields))
    rdd_final = rdd_mapped.keys().distinct()
    print('-------------------------------------------------------------')
    print(f'Número de estaciones distintas en el fichero : {rdd_final.count()}')
    print('-------------------------------------------------------------')
    sc.stop()
    process_logs(applicationId)
    
    
def wide_transformation_sortByKey(conf_parameters, filename, filename_desc):
    conf_parameters = conf_parameters.replace('[', '[sortByKey_')
    sc = SparkContext_app_setup(conf_parameters)
    applicationId = sc.applicationId
    fp_file_input = os.path.join(INPUT_DIR_HDFS, filename)
    rdd_base = sc.textFile(fp_file_input)
    json_fields = get_input_file_fields(filename_desc)
    rdd_mapped = rdd_base.map(lambda x: __init__rdd_mapper(x, json_fields))
    rdd_final = rdd_mapped.map(lambda x: x[20]).sortByKey(True)
    print('-------------------------------------------------------------')
    print(f'Número de estaciones distintas en el fichero : {rdd_final.first()}')
    print('-------------------------------------------------------------')
    sc.stop()
    process_logs(applicationId)

# --------------------- Main wide transformations -----------------


def main(conf_parameters, filename, filename_desc):
    try:
        conf_parameters = conf_parameters.replace("'", '').replace('"', '')
        if '.' in filename:
            file = filename.split(os.sep)[-1]
            app_name = 'app_narrow_transf_' + file
            conf_parameters = conf_parameters.replace('[','[' + app_name + ',')
            #wide_transformation_reduceByKey(str(conf_parameters), filename, filename_desc)
            #wide_transformation_groupByKey(str(conf_parameters), filename, filename_desc)
            #wide_transformation_distinct(str(conf_parameters), filename, filename_desc)
            wide_transformation_sortByKey(str(conf_parameters), filename, filename_desc)
        else:
            SAMPLE_FILES_DIR = os.path.join(INPUT_DIR_HDFS,filename)
            app_name = 'check files in HDFS'
            conf_parameters_tmp = conf_parameters.replace('[','[' + app_name + ',')
            sc = SparkContext_app_setup(conf_parameters_tmp)
            fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
            sample_files = fs.listStatus(sc._jvm.org.apache.hadoop.fs.Path(SAMPLE_FILES_DIR))
            sc.stop()
            for file in sample_files:
                print(f'Applying narrow transformations to filename {file}.')
                file = file.getPath().getName()
                print('-----------------------------------------------------------')
                print(f'filename:{file}')
                print('-----------------------------------------------------------')
                app_name = 'app_narrow_transf_' + file
                filename_final = os.path.join(filename,file)
                conf_parameters_final = conf_parameters.replace('[','[' + app_name + ',')
                for parameter in conf_parameters_final:
                    parameter.replace("'", '')
                #wide_transformation_reduceByKey(str(conf_parameters_final), filename_final, filename_desc)
                #wide_transformation_groupByKey(str(conf_parameters_final), filename_final, filename_desc)
                #wide_transformation_distinct(str(conf_parameters_final), filename_final, filename_desc)
                wide_transformation_sortByKey(str(conf_parameters_final), filename_final, filename_desc)
    except:
        print('------------------------- Error ---------------------------')
        print(f'Unable to process wide_transformations for samples for file {filename}')
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
    