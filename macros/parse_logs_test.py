#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jun 18 19:05:41 2023

@author: Sara García Cabezalí
"""

from VARIABLES import OUTPUT_DIR
import os
import json
import sys
import pandas as pd
from datetime import datetime
from tabulate import tabulate

def info_logs(applicationId):
    fp_logfile = os.path.join(OUTPUT_DIR,'logs', applicationId)
    try:
        with open(fp_logfile, 'r') as app_log_file:
            log_json = json.loads(json.dumps(app_log_file.readlines()))
            events = []
            i, j = 0,0
            reps = {}
            for line in log_json:
                line_json = json.loads(line)
                i += 1
                if 'Event' in line_json:
                    j += 1
                    if line_json['Event'] not in events:
                        events = events + [line_json['Event']]
                        reps[line_json['Event']] = 1
                    else:
                        new = reps[line_json['Event']] + 1
                        reps[line_json['Event']] = new
                        #print(line_json['Event'])
                        #print(line_json)
            print('============================== EVENTS SUMMARY ===============================')
            print('-----------------------------------------------------------------------------')
            print(f' Number of log lines: {i} ......................... Number of Event lines: {j}')
            print('-----------------------------------------------------------------------------')
            for event in events:
                print(f'Event {event} instances: {reps[event]}')
            print('=============================================================================')
            

    except FileNotFoundError:
        print('--------------------ERROR------------------')
        print('Log file not found in logs directory')
        print('-------------------------------------------')
        print('TraceBackError:')
        raise
        
def parse_logs(applicationId):
    fp_logfile = os.path.join(OUTPUT_DIR,'logs', applicationId)
    df = pd.DataFrame(columns = ['Datetime', 'Name', 'Job ID', 'Total Stages',\
                                 'Stage ID', 'Total Tasks', 'Task ID', 'Partition ID',\
                                 'SHUFFLER: Remote Blocks Fetched', 'SHUFFLER: Local Blocks Fetched',\
                                 'SHUFFLER: Fetch Wait Time', 'SHUFFLER: Remote Bytes Read',\
                                 'SHUFFLER: Remote Bytes Read To Disk',\
                                 'SHUFFLER: Local Bytes Read', 'SHUFFLER: Total Records Read',\
                                 'SHUFFLEW: Shuffle Bytes Written', 'SHUFFLEW: Shuffle Records Written',\
                                 'SHUFFLEW: Shuffle Write Time', 'Status'])
    try:
        with open(fp_logfile, 'r') as app_log_file:
            log_json = json.loads(json.dumps(app_log_file.readlines()))
            for line in log_json:
                line_json = json.loads(line)
                #1. APPLICATION START LOGS
                if line_json['Event'] == 'SparkListenerApplicationStart':
                    start_time = datetime.fromtimestamp(int(line_json['Timestamp'])/1000)
                    app_name, app_id, user = str(line_json['App Name']), str(line_json['App ID']), str(line_json['User'])
                    #print('========================= Spark Aplication Start ============================')
                    #print(f'Date and time of {line_json["Event"]}: {start_time}')
                    df.loc[len(df)] = [start_time, 'APPLICATION START',"'-", "'-",\
                           "'-", "'-","'-", "'-","'-", "'-", "'-", "'-","'-", "'-",\
                           "'-", "'-", "'-", "'-", 'SUCCESS']
                #2. APPLICATION END LOGS
                elif line_json['Event'] == 'SparkListenerApplicationEnd':
                    end_time = datetime.fromtimestamp(int(line_json['Timestamp'])/1000)
                    df.loc[len(df)] = [end_time, 'APPLICATION END', "'-", "'-", "'-", "'-","'-", "'-", "'-", "'-", "'-", "'-","'-", "'-", "'-", "'-", "'-", "'-", 'SUCCESS']
                    #print(f'Date and time of {line_json["Event"]}: {end_time}')
                    #print('========================== Spark Aplication End =============================')
                #3. JOB START LOGS
                elif line_json['Event'] == 'SparkListenerJobStart':
                    job_start_date_time = datetime.fromtimestamp(line_json['Submission Time']/1000)
                    job_id = line_json["Job ID"]
                    stage_ids = line_json["Stage IDs"]
                    n_stages = len(stage_ids)
                    #print(f'     ====================== Spark Job {str(job_id)} Start ======================')
                    #print(f'Date and time of {line_json["Event"]}: {job_start_date_time}')
                    #print(f'Number of stages: {n_stages}')
                    #print(f'Stage IDs: {str(stage_ids)}')
                    df.loc[len(df)] = [job_start_date_time, 'JOB START', job_id, n_stages, "'-", "'-", "'-", "'-", "'-", "'-", "'-", "'-","'-", "'-","'-", "'-", "'-", "'-",'SUCCESS']
                #4. JOB END LOGS
                elif line_json['Event'] == 'SparkListenerJobEnd':
                    job_end_date_time = datetime.fromtimestamp(line_json['Completion Time']/1000)
                    status = 'SUCCESS' if line_json['Job Result']['Result'] == 'JobSucceeded' else 'FAIL'
                    #print(f'Date and time of {line_json["Event"]}: {job_end_date_time}')
                    #print(f'                  --------------- {status} ---------------               ')
                    #print(f'Total Job execution time: {(job_end_date_time - job_start_date_time).total_seconds()} seconds.')
                    #print(f'     ======================= Spark Job {str(job_id)} End =======================')
                    df.loc[len(df)] = [job_end_date_time, 'JOB END', job_id, n_stages, "'-", "'-", "'-", "'-", "'-", "'-", "'-", "'-","'-", "'-", "'-", "'-", "'-", "'-", status]
                elif line_json['Event'] == 'SparkListenerStageSubmitted':
                    stage_start_date_time = datetime.fromtimestamp(line_json['Stage Info']['Submission Time']/1000)
                    stage_id = line_json['Stage Info']['Stage ID']
                    n_tasks = line_json['Stage Info']['Number of Tasks']
                    df.loc[len(df)] = [stage_start_date_time, 'STAGE START', job_id, n_stages, stage_id, n_tasks , "'-", "'-", "'-", "'-", "'-", "'-","'-", "'-", "'-", "'-", "'-", "'-", 'SUCCESS']
                    #print(f'......................... Spark Stage {str(stage_id)} Start .........................')
                    #print(f'Date and time of {line_json["Event"]}: {stage_start_date_time}')
                    #print(f'Number of tasks: {n_tasks}')
                elif line_json['Event'] == 'SparkListenerStageCompleted':
                    stage_end_date_time = datetime.fromtimestamp(line_json['Stage Info']['Completion Time']/1000)
                    #print(f'Date and time of {line_json["Event"]}: {stage_end_date_time}')
                    #print(f'Total Stage execution time: {(stage_end_date_time - stage_start_date_time).total_seconds()} seconds.')
                    #print('                  --------------- SUCCESS ---------------               ')
                    #print(f'.......................... Spark Stage {str(stage_id)} End ..........................')
                    df.loc[len(df)] = [stage_end_date_time, 'STAGE END', job_id, n_stages, stage_id, n_tasks , "'-", "'-", "'-", "'-", "'-", "'-","'-", "'-", "'-", "'-", "'-", "'-", 'SUCCESS']
                #5. TASK START LOGS
                elif line_json['Event'] == 'SparkListenerTaskStart':
                    task_start_date_time = datetime.fromtimestamp(line_json['Task Info']['Launch Time']/1000)
                    task_id = line_json['Task Info']['Task ID']
                    partition_id = str(line_json['Task Info']['Partition ID'])
                    status = 'FAIL' if line_json['Task Info']['Failed'] else 'SUCCESS'
                    df.loc[len(df)] = [task_start_date_time, 'TASK START', job_id, n_stages, stage_id, n_tasks , task_id, partition_id, "'-", "'-", "'-", "'-", "'-", "'-", "'-", "'-", "'-", "'-", status]
                    print(line_json)
                    #print(f'    ................... Spark Task {str(task_id)} Start ..................')
                    #print(f'Date and time of {line_json["Event"]}: {job_start_date_time}')
                    #print(f'Partition ID: {partition_id}') 
                #6. TASK END LOGS
                elif line_json['Event'] == 'SparkListenerTaskEnd':
                    task_end_date_time = datetime.fromtimestamp(line_json['Task Info']['Finish Time']/1000)
                    task_id = line_json['Task Info']['Task ID']
                    status = 'FAIL' if line_json['Task Info']['Failed'] else 'SUCCESS'
                    shuffle_read_metrics = line_json['Task Metrics']['Shuffle Read Metrics']
                    shuffle_write_metrics = line_json['Task Metrics']['Shuffle Write Metrics']
                    rm_blocks_fetched = shuffle_read_metrics['Remote Blocks Fetched']
                    l_blocks_fetched = shuffle_read_metrics['Local Blocks Fetched']
                    fetch_wait_time = datetime.fromtimestamp(shuffle_read_metrics['Fetch Wait Time']/1000).second,
                    rm_bytes_read = shuffle_read_metrics['Remote Bytes Read']
                    rm_bytes_read_disk = shuffle_read_metrics['Remote Bytes Read To Disk']
                    l_bytes_read = shuffle_read_metrics['Local Bytes Read']
                    total_records_read = shuffle_read_metrics['Total Records Read']
                    bytes_written = shuffle_write_metrics['Shuffle Bytes Written']
                    records_written = shuffle_write_metrics['Shuffle Records Written']
                    write_time = datetime.fromtimestamp(shuffle_write_metrics['Shuffle Write Time']/1000).second #Comprobar unidades
                    print(f'================= TASK {task_id} ==================')
                    print('-------------- SHUFFLE READ --------------')
                    for key in shuffle_read_metrics:
                        print(key,shuffle_read_metrics[key])
                    print('-------------- SHUFFLE WRITE --------------')
                    for key in shuffle_write_metrics:
                        print(key,shuffle_write_metrics[key])
                    df.loc[len(df)] = [task_end_date_time, 'TASK END', job_id, \
                           n_stages, stage_id, n_tasks , task_id, partition_id,\
                           rm_blocks_fetched, l_blocks_fetched, fetch_wait_time,
                           rm_bytes_read, rm_bytes_read_disk, l_bytes_read,
                           total_records_read, bytes_written, records_written,\
                           write_time, status]
                    #print(f'Date and time of {line_json["Event"]}: {task_end_date_time}')
                    #print(f'                  --------------- {status} ---------------               ')
                    #print(f'Total Task execution time: {(task_end_date_time - task_start_date_time).total_seconds()} seconds.')
        df[['App Name', 'App ID', 'User']] = [app_name,app_id,user]
        first_columns = ['App ID', 'App Name', 'User', 'Datetime', 'Name', 'Job ID',\
                        'Total Stages', 'Stage ID', 'Total Tasks', 'Task ID', 'Partition ID', 'Status']
        rest_columns = [column for column in df.columns if column not in first_columns]
        df = df[first_columns + rest_columns]
        df.astype({'Job ID' : str, 'Stage ID' : str, 'Task ID' : str, 'Partition ID' : str})
        df.sort_values(by = ['Datetime','Job ID', 'Stage ID', 'Task ID'], inplace = True)
        df.to_csv(fp_logfile + '.csv', sep = ';')
        print('==================================== SUMMARY ====================================')
        # Add here summary of complete execution
        #1. Number of stages, tasks and  
        df_summary_1 = df.loc[df['Job ID'] == "'-"]
        df_summary_1 = [[app_name, app_id, user, (end_time - start_time).total_seconds()]]
        print('')
        print('Application summary: ')
        print('')
        print(tabulate(df_summary_1,headers=['App Name','App ID', 'User', 'Execution time (s)'], tablefmt = 'rounded_outline'))
        df_summary_2 = df.drop_duplicates(['Job ID', 'Stage ID', 'Total Tasks'])[['Job ID', 'Stage ID', 'Total Tasks']]
        df_summary_2 = df_summary_2.loc[df_summary_2['Job ID'] != "'-"].loc[df_summary_2['Stage ID'] != "'-"]
        print('')
        print('Jobs, Stages and Tasks summary: ')
        print('')
        print(tabulate(df_summary_2,showindex=False, headers = ['Job ID', 'Stage ID', 'Total Tasks'], tablefmt = 'rounded_outline'))
        print('==================================================================================')
    except FileNotFoundError:
        print('--------------------ERROR------------------')
        print('Log file not found in logs directory')
        print('-------------------------------------------')
        print('TraceBackError:')
        raise

if __name__ == "__main__":
    try:
        assert len(sys.argv) == 2
        #info_logs(sys.argv[1])
        parse_logs(sys.argv[1])
    except AssertionError:
        print('Not properly called script')
    except:
        raise