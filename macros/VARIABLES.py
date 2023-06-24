#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 25 18:31:22 2023

@author: Sara García Cabezalí
"""

import os

# DIRECTORIES DEFINITION (HDFS AND LOCAL CLUSTER)

#   1. LOCAL DIRECTORY CLUSTER
cd = os.getcwd()
project_dir = cd.split(os.sep)[0:len(cd.split(os.sep))-1]
INPUT_DIR = os.path.join(os.sep.join(project_dir), 'input')
OUTPUT_DIR = os.path.join(os.sep.join(project_dir), 'output')
LOG_DIR = os.path.join(OUTPUT_DIR, 'logs')
LOG_DIR_SPARK = os.path.join(os.sep.join(['/opt', 'spark', 'current', 'events']))

#   2. HDFS DIRECTORIES
INPUT_DIR_HDFS = 'hdfs://dana:9000/user/sargar19/input/'
OUTPUT_DIR_HDFS = 'hdfs://dana:9000/user/sargar19/output/'

# -----------------------------------------

TRANSFORMATIONS_APP_PREFIX = {'narrow_transformation_map': 'app_map', \
                             'narrow_transformation_flatMap': 'app_flatMap_',\
                             'narrow_transformation_filter': 'app_filter_',\
                             'narrow_transformation_union': 'app_union_' }