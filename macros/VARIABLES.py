#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 25 18:31:22 2023

@author: Sara García Cabezalí
"""

import os

# DIRECTORIES DEFINITION

cd = os.getcwd()
project_dir = cd.split(os.sep)[0:len(cd)-1]
INPUT_DIR = os.path.join(os.sep.join(project_dir), 'input')
OUTPUT_DIR = os.path.join(os.sep.join(project_dir), 'output')

# -----------------------------------------