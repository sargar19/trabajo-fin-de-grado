#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: Sara García Cabezalí
"""

from main_functions import SparkContext_app_setup, process_logs, __init__rdd_mapper, get_input_file_fields
from VARIABLES import INPUT_DIR_HDFS
import os, sys
from statistics import mean


def GroupByKey_1(conf_parameters, filename, filename_desc):
    conf_parameters = conf_parameters.replace('[', '[GroupByKey_1_')
    sc = SparkContext_app_setup(conf_parameters)
    applicationId = sc.applicationId
    fp_file_input = os.path.join(INPUT_DIR_HDFS, filename)
    json_fields = get_input_file_fields(filename_desc)
    rdd_base = sc.textFile(fp_file_input)
    rdd_mapped = rdd_base.map(lambda x: __init__rdd_mapper(x, json_fields))
    print('-------------------------------------------------------------')
    rdd_mapped = rdd_mapped.map(lambda x: (x[0], 1))
    rdd_group = rdd_mapped.groupByKey().mapValues(sum)
    print(f'El número de resgistros por estación es: {rdd_group.collect()}')
    sc.stop()
    process_logs(applicationId)


def ReduceByKey_1(conf_parameters, filename, filename_desc):
    conf_parameters = conf_parameters.replace('[', '[ReduceByKey_1_')
    sc = SparkContext_app_setup(conf_parameters)
    applicationId = sc.applicationId
    fp_file_input = os.path.join(INPUT_DIR_HDFS, filename)
    json_fields = get_input_file_fields(filename_desc)
    rdd_base = sc.textFile(fp_file_input)
    rdd_mapped = rdd_base.map(lambda x: __init__rdd_mapper(x, json_fields))
    print('-------------------------------------------------------------')
    rdd_mapped = rdd_mapped.map(lambda x: (x[0], 1))
    rdd_reduce = rdd_mapped.reduceByKey(lambda a, b: a+b)
    print(f'El número de resgistros por estación es: {rdd_reduce.collect()}')
    sc.stop()
    process_logs(applicationId)


def GroupByKey_2(conf_parameters, filename, filename_desc):
    conf_parameters = conf_parameters.replace('[', '[GroupByKey_2_')
    sc = SparkContext_app_setup(conf_parameters)
    applicationId = sc.applicationId
    fp_file_input = os.path.join(INPUT_DIR_HDFS, filename)
    json_fields = get_input_file_fields(filename_desc)
    rdd_base = sc.textFile(fp_file_input)
    rdd_mapped = rdd_base.map(lambda x: __init__rdd_mapper(x, json_fields))
    rdd_mapped = rdd_mapped.map(lambda x: (x[0], x[20]))
    rdd_group = rdd_mapped.groupByKey().mapValues(min)
    print('-------------------------------------------------------------')
    print(f'Temperatura mínima histórica por estación: {rdd_group.collect()}')
    print('-------------------------------------------------------------')
    sc.stop()
    process_logs(applicationId)



def ReduceByKey_2(conf_parameters, filename, filename_desc):
    conf_parameters = conf_parameters.replace('[', '[ReduceByKey_2_')
    sc = SparkContext_app_setup(conf_parameters)
    applicationId = sc.applicationId
    fp_file_input = os.path.join(INPUT_DIR_HDFS, filename)
    json_fields = get_input_file_fields(filename_desc)
    rdd_base = sc.textFile(fp_file_input)
    rdd_mapped = rdd_base.map(lambda x: __init__rdd_mapper(x, json_fields))
    print('-------------------------------------------------------------')
    rdd_mapped = rdd_mapped.map(lambda x: (x[0], x[20]))
    rdd_reduce = rdd_mapped.reduceByKey(min)
    print('-------------------------------------------------------------')
    print(f'Las temperaturas mínima y máxima históricas por estación son: {rdd_reduce.collect()}')
    print('-------------------------------------------------------------')
    sc.stop()
    process_logs(applicationId)
    

def GroupByKey_3(conf_parameters, filename, filename_desc):
    conf_parameters = conf_parameters.replace('[', '[GroupByKey_3_')
    sc = SparkContext_app_setup(conf_parameters)
    applicationId = sc.applicationId
    fp_file_input = os.path.join(INPUT_DIR_HDFS, filename)
    json_fields = get_input_file_fields(filename_desc)
    rdd_base = sc.textFile(fp_file_input)
    rdd_mapped = rdd_base.map(lambda x: __init__rdd_mapper(x, json_fields))
    rdd_mapped = rdd_mapped.map(lambda x: (x[0], x[14]))
    rdd_group = rdd_mapped.groupByKey().mapValues(mean)
    print('-------------------------------------------------------------')
    print(f'Las velocidad media histórica del viento de cada estación: {rdd_group.collect()}')
    print('-------------------------------------------------------------')
    sc.stop()
    process_logs(applicationId)



def ReduceByKey_3(conf_parameters, filename, filename_desc):
    conf_parameters = conf_parameters.replace('[', '[ReduceByKey_3_')
    sc = SparkContext_app_setup(conf_parameters)
    applicationId = sc.applicationId
    fp_file_input = os.path.join(INPUT_DIR_HDFS, filename)
    json_fields = get_input_file_fields(filename_desc)
    rdd_base = sc.textFile(fp_file_input)
    rdd_mapped = rdd_base.map(lambda x: __init__rdd_mapper(x, json_fields))
    rdd_mapped = rdd_mapped.map(lambda x: (x[0], x[14]))
    rdd_reduce = rdd_mapped.reduceByKey(mean)
    print('-------------------------------------------------------------')
    print(f'Las velocidad media histórica del viento de cada estación: {rdd_reduce.collect()}')
    print('-------------------------------------------------------------')
    sc.stop()
    process_logs(applicationId)


def main(conf_parameters, filename, filename_desc):
    try:
        conf_parameters = conf_parameters.replace("'", '').replace('"', '')
        if '.' in filename:
            file = filename.split(os.sep)[-1]
            app_name = '"app_narrow_transf_' + file +'"'
            conf_parameters = conf_parameters.replace('[','[' + app_name + ',')
            GroupByKey_1(str(conf_parameters), filename, filename_desc)
            #ReduceByKey_1(str(conf_parameters), filename, filename_desc)
            #GroupByKey_2(str(conf_parameters), filename, filename_desc)
            #ReduceByKey_2(str(conf_parameters), filename, filename_desc)
            #GroupByKey_3(str(conf_parameters), filename, filename_desc)
            #ReduceByKey_3(str(conf_parameters), filename, filename_desc)
        else:
            SAMPLE_FILES_DIR = os.path.join(INPUT_DIR_HDFS,filename)
            app_name = 'check files in HDFS'
            conf_parameters_tmp = conf_parameters.replace('[','[' + app_name + ',')$
            sc = SparkContext_app_setup(conf_parameters_tmp)
            fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
            sample_files = fs.listStatus(sc._jvm.org.apache.hadoop.fs.Path(SAMPLE_FILES_DIR))
            sc.stop()
            for file in sample_files:
                print(f'Applying narrow transformations to filename {file}.')
                file = file.getPath().getName()
                app_name = 'app_narrow_transf_' + file
                filename_final = os.path.join(filename,file)
                conf_parameters_final = conf_parameters.replace('[','[' + app_name + ',')
                for parameter in conf_parameters_final:
                    parameter.replace("'", '')
                GroupByKey_1(str(conf_parameters_final), filename_final, filename_desc)
                #ReduceByKey_1(str(conf_parameters_final), filename_final, filename_desc)
                #GroupByKey_2(str(conf_parameters_final), filename_final, filename_desc)
                #ReduceByKey_2(str(conf_parameters_final), filename_final, filename_desc)
                #GroupByKey_3(str(conf_parameters_final), filename_final, filename_desc)
                #ReduceByKey_3(str(conf_parameters_final), filename_final, filename_desc)
    except:
        print('------------------------- Error ---------------------------')
        print(f'Unable to process GroupByKey and ReduceByKey transformations for file {filename}')
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
    