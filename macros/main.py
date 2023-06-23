#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 20 22:02:07 2023

@author: Sara García Cabezalí
"""

import build_sample
import sys

def main(conf_parameters, filename, filename_desc):
    #1. Build sample files from filename
    print('--------------------- Building samples ---------------------')
    filenames = []
    try:
        for sample_number in range(5):
            for tamaño in [0.01, 0.05, 0.1]:
                filename_prefix = filename[:filename.find('.')]
                sample_filename = filename_prefix + '_' + '_sample_' + str(tamaño).zfill(3).replace('.','') + '_' + str(sample_number+1) + '.txt'
                build_sample.sample_fichero(filename, sample_filename, tamaño, 0)
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
    return(conf_parameters)

if __name__ == "__main__":
    try:
        assert len(sys.argv) == 4
        main(sys.argv[1], sys.argv[2], sys.argv[3])
        print(f'Creación del fichero de tamaño reducido {sys.argv[2]} satisfactoria.')   
    except AssertionError:
        print('')
        print('-------------------------------------------Error------------------------------------------')
        print('Ejecuta el siguiente comando:')
        print('')
        print('                 python3 main.py <spark_conf_parameters> <filename_entrada> <filename_entrada_desc>')
        print('')
        print('   1. <spark_conf_parameters> [array]: parámetros para la configuración del SparkSession:')
        print('          [driver_cores,driver_memory,executor_instances,executor_cores,executor_memory]')
        print('')
        print('   2. <filename_entrada> [str]: nombre del fichero (loc ../input directory) sobre el que aplicar las transformaciones')
        print('')
        print('   3. <filename_entrada_desc> [str]: nombre del fichero con las descripciones de los campos del fichero <filename_entrada>')
        print('')
        print('-----------------------------------------------------------------------------------------')
    except Exception:
        print('')
        # Añadir al log la excepción
        raise