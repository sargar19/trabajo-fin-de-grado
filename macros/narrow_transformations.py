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
    conf_parameters = conf_parameters.replace('[', '[map_')
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
    conf_parameters = conf_parameters.replace('[', '[filter_')
    sc = SparkContext_app_setup(conf_parameters)
    applicationId = sc.applicationId
    fp_file_input = os.path.join(INPUT_DIR_HDFS, filename)
    print(fp_file_input)
    rdd_base = sc.textFile(fp_file_input)
    rdd_mapped = rdd_base.map(lambda x: __init__rdd_mapper(x, filename_desc))
    # En los últimos 10 años, días en los que la temperatura máxima > 40 grados = 106 farenheit
    rdd_filtered = rdd_mapped.filter(lambda x: int(x[1])>= 2013 and float(x[18])>= 104)
    print(f'En los últimos 10 años, la temperatura ha sido superior a 40 grados centigrados y ha llovido en un total de {rdd_filtered.count()} días')
    sc.stop()
    process_logs(applicationId)
        
        
def narrow_transformation_union(conf_parameters, filename, filename_desc):
    sc = SparkContext_app_setup(conf_parameters)
    fp_file_input = os.path.join(INPUT_DIR_HDFS, filename)
    rdd_base = sc.textFile(fp_file_input)
    return(rdd_base)


# --------------------- Main map transformations -----------------


def main(conf_parameters, filename, filename_desc):
    try:
        conf_parameters = conf_parameters.replace("'", '').replace('"', '')
        if '.' in filename:
            file = filename.split(os.sep)[-1]
            app_name = '"app_narrow_transf_' + file +'"'
            conf_parameters = conf_parameters.replace('[','[' + app_name + ',')
            narrow_transformation_map(str(conf_parameters), filename, filename_desc)
            #narrow_transformation_filter(str(conf_parameters), filename, filename_desc)
            #narrow_transformation_union(str(conf_parameters), filename, filename_desc)
        else:
            SAMPLE_FILES_DIR = os.path.join(INPUT_DIR_HDFS,filename)
            app_name = 'check files in HDFS'
            conf_parameters_tmp = conf_parameters.replace('[','[' + app_name + ',')
            with SparkContext_app_setup(conf_parameters_tmp) as sc:
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
                narrow_transformation_map(str(conf_parameters_final), filename_final, filename_desc)
                #narrow_transformation_filter(str(conf_parameters_final), filename_final, filename_desc)
                #narrow_transformation_union(str(conf_parameters_final), filename_final, filename_desc)
    except:
        print('------------------------- Error ---------------------------')
        print(f'Unable to process narrow_transfromations for samples for file {filename}')
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
    