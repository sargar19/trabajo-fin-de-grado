#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jun 24 20:17:25 2023

@author: Sara García Cabezalí
"""

from narrow_transformations import apply_narrow_transformations
from VARIABLES import INPUT_DIR_HDFS
from main_functions import SparkContext_app_setup
import sys,os

def main(conf_parameters, filename, filename_desc):
    try:
        conf_parameters = conf_parameters.replace("'", '').replace('"', '')
        if '.' in filename:
            file = filename.split(os.sep)[-1]
            app_name = '"app_narrow_transf_' + file +'"'
            conf_parameters = conf_parameters.replace('[','[' + app_name + ',')
            apply_narrow_transformations(str(conf_parameters), filename, filename_desc)
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
                app_name = 'app_narrow_transf_' + file
                filename_final = os.path.join(filename,file)
                conf_parameters_final = conf_parameters.replace('[','[' + app_name + ',')
                for parameter in conf_parameters_final:
                    parameter.replace("'", '')
                print('----------------------')
                print(conf_parameters_final)
                print('----------------------')
                apply_narrow_transformations(str(conf_parameters_final), filename_final, filename_desc)
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
        print('       python3 main_narrow_transformations.py <spark_conf_parameters> <filename_entrada / files_directory> <filename_entrada_desc>')
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
    