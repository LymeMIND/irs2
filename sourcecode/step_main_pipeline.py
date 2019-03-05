from sqlalchemy import create_engine
import os
import argparse

###
##AUTOGENERATE
# INTERNAL LIBRARY
###
#SELF GENERATE IMPORT BASED ON TASK SELECTION import fastqc import star import picard .. globals()['nome funzione']

from step_star import entry_point_star
from step_picard1 import entry_point_picard1
from step_picard2 import entry_point_picard2
from step_features_count import entry_point_features_count
####


def run_pipeline(pipeline_id,run_dry=True,resetquery=True,one_sample=True):

    pg_user             = os.environ.get('DB_USER_LYME')
    pg_password         = os.environ['DB_PASSWORD_LYME']
    pg_host             = os.environ['DB_HOST_LYME']
    pg_conn_str         = 'postgresql://' + pg_user + ':' + pg_password + '@' + pg_host + ':5432/rnaseq_manager'
    pg_conn             = create_engine(pg_conn_str, echo=False, paramstyle='format', pool_recycle=1800)
    #read step for the pipeline

    select_tasks_pipeline  = 'SELECT pipeline_run_task.task_id, next_task,sequence, func_name FROM pipeline_run_task INNER JOIN task ON pipeline_run_task.task_id = task.task_id '\
    'WHERE pipeline_run_task.pipeline_id=%d ORDER BY sequence;' % (pipeline_id)
    task2performe=pg_conn.execute(select_tasks_pipeline).fetchall()

    select_project_name = 'SELECT sinai_id FROM pipeline WHERE pipeline_id = %d' % (pipeline_id)
    pipeline_info = pg_conn.execute(select_project_name).fetchall()

    if(len(pipeline_info) == 1):
        project_name = pipeline_info[0][0]
        function_call='entry_point_star'
        for current_task in task2performe:

            if(current_task[1] == -1):
                last_task = current_task
            else:
                task_id = current_task[0]
                next_task_id = current_task[1]
                function_call = current_task[-1]
                print('########################')
                print(function_call)
                print('########################')
                globals()[function_call](pg_conn,project_name ,pipeline_id,task_id,next_task_id, run_dry,resetquery,one_sample)
            #     # step_star.entry_point_star(pg_conn,project_name, pipeline_id,task_id,next_task_id)
            #     break


        print(last_task)
        function_call = last_task[-1]
        task_id = last_task[0]
        next_task_id = last_task[1]
        print('########################')
        print('performe last task')
        print(function_call)
        print('########################')
        globals()[function_call](pg_conn,project_name ,pipeline_id,task_id,next_task_id, run_dry=False)
        # step.step_generic(pg_conn,pipeline_id,last_task[0], next_task_id = -1)

    else:
        print('ERROR with PIPELINE SELECTED')
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--pipeline', required=True ,help='which pipeline to run', type=int)
    parser.add_argument('--run_dry', help='true to run without updating the DB [{0,False}|{1,True} ]')
    parser.add_argument('--resetquery' ,help='true to run without updating the DB [{0,False}|{1,True} ]')
    parser.add_argument('--one_sample' ,help='true to run without updating the DB [{0,False}|{1,True} ]')
    args = parser.parse_args()
    pipeline_id= args.pipeline
    run_dry = args.run_dry
    resetquery = args.resetquery
    one_sample = args.one_sample


    if(run_dry in ['0',0,'False']):
        run_dry=False
    elif (run_dry in ['1',1,'True']):
        run_dry=True
    else:
        print('Invalid value for run_dry')
        exit()

    if(resetquery in ['0',0,'False']):
        resetquery=False
    elif (resetquery in ['1',1,'True']):
        resetquery=True
    else:
        print('Invalid value for run_dry')
        exit()

    if(one_sample in ['0',0,'False']):
        one_sample=False
    elif (one_sample in ['1',1,'True']):
        one_sample=True
    else:
        print('Invalid value for run_dry')
        exit()

    run_pipeline(pipeline_id,run_dry,resetquery,one_sample)

