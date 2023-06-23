#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May 23 12:11:57 2023

@author: Sara García Cabezalí
"""

import sys
from VARIABLES import INPUT_DIR
from main_functions import SparkContext_app_setup
import os

def sample_fichero(filename_entrada, filename_salida, tamaño, header):
    fp_filename_entrada = os.path.join(INPUT_DIR, filename_entrada)
    OUTPUT_DIR = os.path.join(INPUT_DIR, filename_entrada[:filename_entrada.find('.')] + '_samples')
    os.system('hdfs dfs -mkdir {OUTPUT_DIR}')
    TMP_OUTPUT_DIR = os.path.join(INPUT_DIR, 'tmp')
    conf_parameters = "['build_sample_example',1,1g,2,2,6g]"
    sc = SparkContext_app_setup(conf_parameters)
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
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
    list_status = fs.listStatus(sc._jvm.org.apache.hadoop.fs.Path(TMP_OUTPUT_DIR))
    print("=======================================")
    for file in list_status:
        filename_spark = file.getPath().getName()
        if filename_spark.startswith('part-'):
            print(f'Sample file of size {float(tamaño)*100}% from original file {filename_entrada} built in directory {OUTPUT_DIR} under filename {filename_spark}')
            break
    print("=======================================")
    os.system(f'hdfs dfs -mv {os.path.join(TMP_OUTPUT_DIR, filename_spark)} {os.path.join(OUTPUT_DIR, filename_salida)}')
    """fs.rename(
        sc._jvm.org.apache.hadoop.fs.Path(os.path.join(TMP_OUTPUT_DIR, filename_spark)),
        sc._jvm.org.apache.hadoop.fs.Path(os.path.join(OUTPUT_DIR, filename_salida))
    )"""
    fs.delete(sc._jvm.org.apache.hadoop.fs.Path(TMP_OUTPUT_DIR), True)
    print(f'Sample file {filename_spark} renamed to desired filename: {filename_salida}')
    sc.stop()
    
if __name__ == "__main__":
    try:
        assert len(sys.argv) == 5
        sample_fichero(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
        print(f'Creación del fichero de tamaño reducido {sys.argv[2]} satisfactoria.')
    except AssertionError:
        print('------------------------------------- ERROR ----------------------------------------')
        print('Ejecuta el siguiente comando:')
        print('')
        print('    python3 fichero_reducido.py <filename_entrada> <filename_salida> <tamaño> <header>')
        print('')
        print('   1. <filename_entrada> [str]: numbre del fichero para samplear (localizado en hdfs user/sargar19/input/)')
        print('')
        print('   2. <filename_salida> [str]: nombre deseado para el fichero sampleado')
        print('')
        print('   3. <tamaño> [str]: nombre del fichero con las descripciones de los campos del fichero <filename_entrada>')
        print('')
        print('   4. <header> [bool]: 1 si el fichero contiene como primera línea los nombres de los campos del fichero - 0 en caso contrario')
        print('')
        print('-----------------------------------------------------------------------------------')
    except FileNotFoundError:
        print('------------------------------------- ERROR ----------------------------------------')
        print(f'No se encuentra el fichero {sys.argv[1]}. Por favor, comprueba la existencia de este.')
        print('------------------------------------------------------------------------------------')
        raise