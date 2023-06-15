#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May 23 12:11:57 2023

@author: Sara García Cabezalí
"""

import sys
import random

def sample_fichero(filename_entrada, filename_salida, tamaño, header):
    with open(filename_entrada, 'r') as f:
        with open(filename_salida, 'w') as g:
            for line_number, line in enumerate(f):
                rand = random.random()
                if line_number == 0 and header:
                    g.write(line)
                elif rand <= float(tamaño):
                    g.write(line)
                else:
                    continue
            g.close()
        f.close()

if __name__ == "__main__":
    try:
        assert len(sys.argv) == 5
        sample_fichero(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
        print(f'Creación del fichero de tamaño reducido {sys.argv[2]} satisfactoria.')
    except AssertionError:
        print('Ejecuta el siguiente comando: python3 fichero_reducido.py <filename_entrada> <filename_salida> <tamaño> <0: header, 1: no header>')
    except FileNotFoundError:
        print(f'No se encuentra el fichero {sys.argv[1]}. Por favor, comprueba la existencia de este.')