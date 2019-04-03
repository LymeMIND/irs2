from sqlalchemy import create_engine
import os
import time
import datetime
import argparse




def entry_point_fastqc(pg_conn,project_code,pipeline_id,task_id,next_task_id,mount_point,run_dry=True,resetquery=True,one_sample=True):

    select_option_task = 'SELECT option_name,option_value FROM option INNER JOIN task ON option.task_id = task.task_id WHERE task.task_id=%d;'
    select_info_task='SELECT path,command, output_directory, table_name FROM task WHERE task_id=%d'

    query            = select_option_task % task_id
    options          = pg_conn.execute(query).fetchall()
    options_test     =   {c.option_name:c.option_value for c in options}
    options_template =' '.join(option+' ' + options_test[option] for option in options_test)


    query = select_info_task % (task_id)
    results_task_query = pg_conn.execute(query).fetchall()


    path_value  = results_task_query[0][0]
    command     = results_task_query[0][1]
    output_directory_db    = results_task_query[0][2]
    current_table_name = results_task_query[0][3]

    output_directory_db = ('{}/{}').format(output_directory_db, project_code)
    output_directory = ('{}{}').format(mount_point[:-1],output_directory_db)
    if(not os.path.exists(output_directory)):
        os.mkdir(output_directory)

    if(next_task_id != -1):
        query = select_info_task % (next_task_id)
        results_task_query = pg_conn.execute(query).fetchall()
        next_table_name = results_task_query[0][3]
        step_fastqc(pg_conn, pipeline_id,task_id,next_task_id,current_table_name, next_table_name,path_value,command,output_directory,output_directory_db,mount_point,run_dry,resetquery,one_sample)
        #call the right function here!
    else:
        print('LAST step')


def step_fastqc(pg_conn,pipeline_id,task_id,next_task_id, current_table,next_table,path,command,output_directory,output_directory_db,mount_point,run_dry=True,resetquery=True,one_sample=True):

    select_option_fastqc ='SELECT option_name,option_value FROM option INNER JOIN task ON option.task_id = task.task_id WHERE task.task_id=%d;' % (task_id)
    options = pg_conn.execute(select_option_fastqc).fetchall()


    options_test = {c.option_name:c.option_value for c in options}
    options_template =' '.join(option+' ' + options_test[option] for option in options_test)

##queries
    sample_selection          = 'SELECT Distinct sample_id, dir_input,filename_input, end_type '\
                          'FROM %s '\
                          'WHERE pipeline_id=%d AND task_id=%d AND status=\'%s\'  ORDER BY sample_id LIMIT 1'

    query_select_paired = 'SELECT filename_input FROM %s WHERE pipeline_id=%d AND task_id=%d AND sample_id=\'%s\' '

    sample_update_process       = 'UPDATE %s SET status=\'%s\' '\
                                'WHERE  pipeline_id=%d AND sample_id=\'%s\' AND filename_input=\'%s\' AND task_id=%d'
    sample_update_add_process   = 'UPDATE %s SET status=\'%s\', date=\'%s\' , run_time=\'%s\',dir_output=\'%s\', filename_output=\'%s\' '\
                                  'WHERE  pipeline_id=%d AND sample_id=\'%s\' AND filename_input=\'%s\' AND task_id=%d'



    query_insert_next_step = 'INSERT INTO %s (pipeline_id,task_id,sample_id,dir_input,filename_input,dir_output,filename_output,status,date, run_time, trimmed_quality, end_type,qc ) '\
    'VALUES(%d,%d,\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',%d,%d, \'%s\', \'passed\')'
    query_update_next_step = 'UPDATE  %s SET dir_input=\'%s\', filename_input=\'%s\', dir_output=\'%s\', filename_output=\'%s\','\
            ' status=\'%s\' ,date=\'%s\',run_time=\'%s\', trimmed_quality=%d WHERE filename_input=\'%s\' AND pipeline_id=%d AND task_id=%d AND sample_id=\'%s\''

    #STAR
    task_program = ''.join([path,command])


    query = sample_selection %(current_table,pipeline_id, task_id,'pending')
    samples = pg_conn.execute(query).fetchall()

    print(query)
    while(len(samples) > 0):

        sample          = samples[0]
        #############################
        sample_id       = sample[0]
        dir_input       = sample[1]
        filename_input  = sample[2]
        end_type        = sample[3]


        sample2run = ' '.join([task_program, options_template])


        dir2check = ('{}{}/{}').format(mount_point,dir_input,sample_id)
        if(end_type =='single'):
            query = sample_update_process %(current_table, 'running',pipeline_id,sample_id, filename_input,task_id)
            if(run_dry):
                print(query)
            else:
                pg_conn.execute(query)

            samplefile = ('{}/{}').format(dir2check,filename_input)
            filenames_input  = [samplefile]
            only_filename   = [filename_input]
        else:


            query2run = query_select_paired %( current_table,pipeline_id,task_id,sample_id)
            samples_paired = pg_conn.execute(query2run).fetchall()
            filenames_input = []
            only_filename   = []
            for ff in samples_paired:
                filenames_input.append(os.path.join(dir2check, ff[0]))
                only_filename.append(ff[0])
                query = sample_update_process %(current_table, 'running',pipeline_id,sample_id, ff[0],task_id)
                if(run_dry):
                    print(query)
                else:
                    pg_conn.execute(query)

        input_fastqc = ' '.join(filenames_input)
        output_fastqc = ' -o {}'.format(output_directory)

        #######################
        sample2run=' '.join([sample2run, input_fastqc, output_fastqc])

        start_time = time.time()
        print(sample2run)

        if(not run_dry):
            os.system(sample2run)

        elapse  = time.time() - start_time
        date    = datetime.datetime.today().strftime('%Y-%m-%d')
        run_time_next   = 0
        trimmed_quality = 0

        for filename_input in only_filename:

            filename_output = filename_input.split('.')[0]+'_fastqc.html'
            query_update= sample_update_add_process % (current_table,'done',date,int(elapse),output_directory_db ,filename_output ,pipeline_id,sample_id, filename_input,task_id)
            if(run_dry):
                print(query_update)
            else:
                pg_conn.execute(query_update)

            query_insert = query_insert_next_step % (next_table, pipeline_id,next_task_id,sample_id, dir_input, filename_input,'null','null','pending', date, run_time_next, trimmed_quality, end_type)
            query_update = query_update_next_step % (next_table,dir_input, filename_output,'null','null','pending', date, run_time_next,trimmed_quality ,filename_output,pipeline_id,next_task_id,sample_id)

            if(run_dry):
                print(query_insert)
                print(query_update)
            else:
                try:
                    pg_conn.execute(query_insert)
                except Exception as e:
                    # print(e)
                    try:
                        pg_conn.execute(query_update)
                    except Exception as e:
                        print(e)


        query_check = sample_selection % (current_table,pipeline_id, task_id,'pending')
        samples = pg_conn.execute(query_check).fetchall()

        if(one_sample):
            break




    if(resetquery == True):
        samples_update_process = 'UPDATE  %s SET status=\'pending\' WHERE pipeline_id=%d AND task_id=%d'  %(current_table,pipeline_id,task_id)
        print(samples_update_process)
        pg_conn.execute(samples_update_process)






if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', required=True ,help=' in which project fecth samples ')
    parser.add_argument('--pipeline', required=True ,help='which pipeline to run')
    parser.add_argument('--task',required=True ,help='current task to run')
    parser.add_argument('--next_task',required=True ,help='next task to run')

    args = parser.parse_args()
    project = args.project
    pipeline_id= int(args.pipeline)
    task_id = int(args.task)
    next_task_id = int(args.next_task)
##Database connection
    pg_user             = os.environ.get('DB_USER_LYME')
    pg_password         = os.environ['DB_PASSWORD_LYME']
    pg_host             = os.environ['DB_HOST_LYME']
    pg_conn_str         = 'postgresql://' + pg_user + ':' + pg_password + '@' + pg_host + ':5432/rnaseq_manager'
    pg_conn             = create_engine(pg_conn_str, echo=False, paramstyle='format', pool_recycle=1800)

    entry_point_fastqc(pg_conn,project,pipeline_id,task_id,next_task_id,run_dry=False,resetquery=True,one_sample=True)
