

from sqlalchemy import create_engine
import os
import time
import datetime
import argparse




def entry_point_fastqc(pg_conn, project_name,pipeline_id,task_id, next_task_id,run_dry=True,resetquery=True,one_sample=True):

    select_option_task  = 'SELECT option_name,option_value FROM option INNER JOIN task ON option.task_id = task.task_id WHERE task.task_id=%d;'
    select_info_task    ='SELECT path,command, output_directory,table_name FROM task WHERE task_id=%d'

    query   = select_option_task % task_id
    options = pg_conn.execute(query).fetchall()
    options_test={c.option_name:c.option_value for c in options}
    options_template =' '.join(option+' ' + options_test[option] for option in options_test)

    query = select_info_task % (task_id)
    results_task_query = pg_conn.execute(query).fetchall()

    path                = results_task_query[0][0]
    command             = results_task_query[0][1]
    output_directory    = results_task_query[0][2]
    current_table_name  = results_task_query[0][3]

    # output_folder = '/data2/projects/IRS2/FASTQC/%s/' % project_name
    if(next_task_id != -1):

        print(current_table_name)
        query = select_info_task % (next_task_id)
        results_task_query = pg_conn.execute(query).fetchall()
        next_table_name = results_task_query[0][3]
        print(next_table_name)
        step_fastqc(pg_conn,project_name ,pipeline_id,task_id,next_task_id,current_table_name, next_table_name,path,command,output_directory,run_dry=False)
        #call the right function here!
    else:
        print('LAST step')



def step_fastqc(pg_conn,project_name ,pipeline_id,task_id,next_task_id,current_table, next_table_name,path,command,output_folder,run_dry=True,resetquery=True,one_sample=True):

    select_option_star='SELECT option_name,option_value FROM option INNER JOIN task ON option.task_id = task.task_id WHERE task.task_id=%d;' % (task_id)
    options=pg_conn.execute(select_option_star).fetchall()


    options_test={c.option_name:c.option_value for c in options}
    options_template =' '.join(option+' ' + options_test[option] for option in options_test)

##queries
    sample_selection          = 'SELECT Distinct sample_id, directory,filename, end_type '\
                          'FROM %s '\
                          'WHERE project_name=\'%s\' AND status=\'%s\' ORDER BY sample_id LIMIT 1'

    sample_update_process       = 'UPDATE %s SET status=\'%s\' '\
                                'WHERE  project_name=\'%s\' AND sample_id=\'%s\' AND filename=\'%s\' ;'




    query_insert_fastqc_tmp = 'INSERT INTO %s (pipeline_id,task_id,sample_id,dir_input,filename_input,dir_output,filename_output,status,date, run_time,end_type ) VALUES(%d,%d,\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',%f,\'%s\')'
    query_update_fastqc_tmp = 'UPDATE  %s SET dir_input=\'%s\', filename_input=\'%s\', dir_output=\'%s\', filename_output=\'%s\','\
            'date=\'%s\',run_time=%f WHERE filename_input=\'%s\' AND pipeline_id=%d AND task_id=%d AND sample_id=\'%s\''

    query_insert_next_step_tmp   = 'INSERT INTO %s (pipeline_id,task_id,sample_id,dir_input,filename_input,dir_output,filename_output,status,date, run_time,end_type) VALUES(%d,%d,\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',%f, \'%s\')'

    task_program = ''.join([path,command])


    print(task_program)
    query = sample_selection %(current_table,project_name, 'pending')
    try:
        print(query)
        samples = pg_conn.execute(query).fetchall()
    except Exception as e:
        print(e)
        print('error')


    table_fastqc = 'file_process_fastqc'
    while(len(samples) > 0):
        sample = samples[0]

        sample_id       = sample[0]
        dir_input       = sample[1]
        filename_input  = sample[2]
        end_type        = sample[3]

        samplefile = ('%s/%s/%s') % (dir_input,sample_id,filename_input)
        print(samplefile)
        dir_output=('%s%s') % (output_folder, sample_id)

        if(os.path.isfile(samplefile)):
            sample2run = ' '.join([task_program, options_template])
            if(end_type =='single'):
                print('single')
                query = sample_update_process % (current_table,'running', project, sample_id, filename_input)
                if(run_dry):
                    print(query)
                else:
                    pg_conn.execute(query)
                options_change  = samplefile


            else:
                print('double')
                query = sample_update_process % ('running', current_table, project, sample_id, filename_input)
                if(run_dry):
                    print(query)
                else:
                    pg_conn.execute(query)



            sample2run=' '.join([sample2run, options_change])
            if(not os.path.exists(dir_output)):
                try:
                    os.mkdir(dir_output)
                except OSError:
                    print ("Creation of the directory %s failed" % dir_output)
                else:
                    print ("Successfully created the directory %s " % dir_output)
            else
                print('Directory:%s ALREADY EXIST' % dir_output)

            start_time = time.time()

            # if(not run_dry):
            #     os.system(sample2run)

            print(sample2run)
            elapse=time.time() - start_time
            print(elapse)
            date=datetime.datetime.today().strftime('%Y-%m-%d')
            run_time_next = 0
            if(end_type =='single'):
                filename_output = filename_input.replace('.fastq.gz','_fastqc.html')
                query_insert_fastqc = query_insert_fastqc_tmp % (table_fastqc, pipeline_id,task_id,sample_id, dir_input ,filename_input, dir_output, filename_output,'done', date, elapse,end_type)
                query_update_fastqc = query_update_fastqc_tmp % (table_fastqc,dir_input, filename_input,dir_output, filename_output, date, elapse,filename_input,pipeline_id,task_id,sample_id)



                query_insert_next_step   = query_insert_next_step_tmp % (next_table_name, pipeline_id,next_task_id,sample_id, dir_input, filename_input ,'null','null','pending', date, run_time_next,end_type)

                if(run_dry):
                    print(query_insert_fastqc)
                    print(query_insert_next_step)
                else:
                    try:
                        pg_conn.execute(query_insert_fastqc)
                    except Exception as e:
                        try:
                            pg_conn.execute(query_update_fastqc)
                        except Exception as e:
                            print(e)

                    try:
                        pg_conn.execute(query_insert_next_step)
                    except Exception as e:
                        print('SAMPLE ALREADY EXIST INTO THE NEXT TABLE TASK')
                        print(query_insert_next_step)

            else:
                print('double')




        if(one_sample):
            break
        query_check = sample_selection %(current_table,project_name, 'pending')
        samples = pg_conn.execute(query_check).fetchall()

    if(resetquery == True):
        samples_update_process = 'UPDATE %s SET status=\'pending\' WHERE project_name=\'%s\'' %(current_table,project)
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

    pg_user             = os.environ.get('DB_USER_LYME')
    pg_password         = os.environ['DB_PASSWORD_LYME']
    pg_host             = os.environ['DB_HOST_LYME']
    pg_conn_str         = 'postgresql://' + pg_user + ':' + pg_password + '@' + pg_host + ':5432/rnaseq_manager'
    pg_conn             = create_engine(pg_conn_str, echo=False, paramstyle='format', pool_recycle=1800)

    entry_point_fastqc(pg_conn,project,pipeline_id ,task_id ,next_task_id,run_dry=False,resetquery=False,one_sample=False)
