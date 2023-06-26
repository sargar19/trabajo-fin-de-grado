#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May 23 12:11:57 2023

@author: Sara García Cabezalí
"""
import sys, os, subprocess
from VARIABLES import INPUT_DIR_HDFS
from main_functions import SparkContext_app_setup, process_logs
import numpy as np


def sample_fichero(filename_entrada, filename_salida, tamaño, header):
    fp_filename_entrada = os.path.join(INPUT_DIR_HDFS, filename_entrada)
    OUTPUT_DIR = os.path.join(INPUT_DIR_HDFS, filename_entrada[:filename_entrada.find('.')] + '_samples')
    #os.system('hdfs dfs -mkdir {OUTPUT_DIR}')
    TMP_OUTPUT_DIR = os.path.join(INPUT_DIR_HDFS, 'tmp')
    conf_parameters = "['build_sample_example',1,1g,2,2,6g]"
    sc = SparkContext_app_setup(conf_parameters)
    applicationId = sc.applicationId
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
    list_status = fs.listStatus(sc._jvm.org.apache.hadoop.fs.Path(INPUT_DIR_HDFS))
    if filename_entrada[:filename_entrada.find('.')] + '_samples' in list_status:
        pass
    else:
        subprocess.run(['hdfs','dfs','-mkdir', OUTPUT_DIR])
    rdd_data = sc.textFile(fp_filename_entrada)
    rdd_data = rdd_data.repartition(10)
    if header:
        rdd_data = rdd_data.map(lambda x: (1, x))
        header_line = rdd_data.filter(lambda x: x[0] == 1).map(lambda x: x[1])
        rdd_data = rdd_data.filter(lambda x: x[0] != 1).map(lambda x: x[1])
        rdd_sample = rdd_data.sample(withReplacement = False, fraction = float(tamaño))
        rdd_sample = header_line.union(rdd_sample)
    else:
        rdd_sample = rdd_data.sample(withReplacement = False, fraction = float(tamaño))
    rdd_sample.repartition(1).saveAsTextFile(TMP_OUTPUT_DIR)
    list_status = fs.listStatus(sc._jvm.org.apache.hadoop.fs.Path(TMP_OUTPUT_DIR))
    print("=======================================")
    for file in list_status:
        filename_spark = file.getPath().getName()
        if filename_spark.startswith('part-'):
            print(f'Sample file of size {float(tamaño)*100}% from original file {filename_entrada} built in directory {TMP_OUTPUT_DIR} under filename {filename_spark}')
            break
    print("=======================================")
    subprocess.run(['hdfs','dfs','-mv', os.path.join(TMP_OUTPUT_DIR, filename_spark), os.path.join(OUTPUT_DIR, filename_salida)])
    fs.delete(sc._jvm.org.apache.hadoop.fs.Path(TMP_OUTPUT_DIR), True)
    print(f'Sample file {filename_spark} renamed to desired filename: {filename_salida}')
    sc.stop()
    process_logs(applicationId)

def main(filename, tamaños, samples_number):
    #1. Build sample files from filename
    print('--------------------- Building samples ---------------------')
    filenames = []
    tamaños = [float(x) for x in tamaños.strip('][').split(',')]
    try:
        for sample_number in range(int(samples_number)):
            for tamaño in np.arange(0.01,1,0.01):
                filename_prefix = filename[:filename.find('.')]
                sample_filename = filename_prefix + '_sample_' + str(tamaño).zfill(3).replace('.','') + '_' + str(sample_number+1) + '.txt'
                sample_fichero(filename, sample_filename, tamaño, 0)
                filenames = filenames + [sample_filename]
        print('-------------------- Samples built -------------------------')
        print(filenames)
        """try:
            for filename in filenames:
                app_name = 'app_narrow_transf_' + sample_filename
                conf_parameters = [app_name] + conf_parameters
                narrow_transformations(conf_parameters, filename, filename_desc)
        except:
            raise"""
    except:
        print('------------------------- Error ---------------------------')
        print(f'Unable to build samples for file {filename}')
        raise

if __name__ == "__main__":
    try:
        assert len(sys.argv) == 4
        main(sys.argv[1], sys.argv[2], sys.argv[3])
        print(f'Creación del fichero de tamaño reducido {sys.argv[2]} satisfactoria.')   
    except AssertionError:
        print('------------------------------------- ERROR ----------------------------------------')
        print('Ejecuta el siguiente comando:')
        print('')
        print('    python3 build_sample_hdfs.py <filename_entrada> <filename_salida> <tamaño> <header>')
        print('')
        print('   1. <filename> [str]: numbre del fichero para samplear (localizado en hdfs user/sargar19/input/)')
        print('')
        print('   2. <tamaños> [lst]: list de valores xi (float) entre 0 y 1 tales que size(sample_file) = xi * 100 % size(filename)')
        print('         Ejemplo: [0.01,0.1] --> construirá 2 samples con 1% y 10%, respectivamente, del tamaño del fichero original')
        print('')
        print('   3. <samples_number> [int]: número de ficheros samples de cada uno de los tamaños anteriores.')
        print('')
        print('-----------------------------------------------------------------------------------')
    except Exception:
        print('')
        raise
    except FileNotFoundError:
        print('------------------------------------- ERROR ----------------------------------------')
        print(f'No se encuentra el fichero {sys.argv[1]}. Por favor, comprueba la existencia de este.')
        print('------------------------------------------------------------------------------------')
        raise

