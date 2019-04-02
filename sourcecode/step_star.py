
from sqlalchemy import create_engine
import os
import time
import datetime
import argparse



def entry_point_star(pg_conn,project_code, pipeline_id,task_id,next_task_id,run_dry=True,resetquery=True,one_sample=True):

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


    #CHECK IF EXIST
    output_directory = ('{}/{}').format(output_directory, project_code)
    if(not os.path.exists(output_directory)):
        os.mkdir(output_directory)

    if(next_task_id != -1):
        query = select_info_task % (next_task_id)
        results_task_query = pg_conn.execute(query).fetchall()
        next_table_name = results_task_query[0][3]
        step_star(pg_conn, pipeline_id,task_id,next_task_id,current_table_name, next_table_name,path,command,output_directory, run_dry,resetquery,one_sample)
        #call the right function here!
    else:
        print('LAST step')


def checkoutput(dir_output,sample_id):
    filename= '{}/{}Log.out'.format(dir_output,sample_id)
    try:
        fp = open(filename)
        alllines=fp.readlines()
        last = alllines[-1].strip('\n')
        if(last == 'ALL DONE!'):
            return 1
        else:
            return -1
    except Exception as e:
        print(e)
        exit()



def step_star(pg_conn, pipeline_id,task_id,next_task_id,current_table, next_table,path,command,output_folder,run_dry=True,resetquery=True,one_sample=True):


    select_option_star='SELECT option_name,option_value FROM option INNER JOIN task ON option.task_id = task.task_id WHERE task.task_id=%d;' % (task_id)
    options=pg_conn.execute(select_option_star).fetchall()


    options_test={c.option_name:c.option_value for c in options}
    options_template =' '.join(option+' ' + options_test[option] for option in options_test)

##queries
    sample_selection          = 'SELECT Distinct sample_id, dir_input,filename_input, end_type,qc,trimmed_quality '\
                          'FROM %s '\
                          'WHERE pipeline_id=%d AND status=\'%s\' AND task_id=\'%s\' ORDER BY sample_id LIMIT 1'

    query_select_paired = 'SELECT filename_input FROM %s WHERE pipeline_id=%d AND task_id=%d AND sample_id=\'%s\''

    sample_update_process       = 'UPDATE %s SET status=\'%s\' '\
                                'WHERE  pipeline_id=%d AND sample_id=\'%s\' AND filename_input=\'%s\' AND task_id=%d'
    sample_update_add_process   = 'UPDATE %s SET status=\'%s\', date=\'%s\' , run_time=\'%s\',dir_output=\'%s\', filename_output=\'%s\' '\
                                  'WHERE  pipeline_id=%d AND sample_id=\'%s\' AND filename_input=\'%s\' AND task_id=%d'



    query_insert_next_step = 'INSERT INTO %s (pipeline_id,task_id,sample_id,dir_input,filename_input,dir_output,filename_output,status,date, run_time, trimmed_quality ) VALUES(%d,%d,\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',%d,%d)'
    query_update_next_step = 'UPDATE  %s SET dir_input=\'%s\', filename_input=\'%s\', dir_output=\'%s\', filename_output=\'%s\','\
            ' status=\'%s\' ,date=\'%s\',run_time=\'%s\', trimmed_quality=%d WHERE filename_input=\'%s\' AND pipeline_id=%d AND task_id=%d AND sample_id=\'%s\''

    #STAR
    task_program = ''.join([path,command])


    query = sample_selection %(current_table,pipeline_id, 'pending',task_id)
    samples = pg_conn.execute(query).fetchall()
    while(len(samples) > 0):
        ctrl = 0
        sample = samples[0]

        sample_id       = sample[0]
        dir_input       = sample[1]
        filename_input  = sample[2]
        end_type        = sample[3]
        qc              = sample[4]
        trimmed_quality = sample[5]

        samplefile = ('%s/%s/%s') % (dir_input,sample_id,filename_input)
        print(samplefile)


        if(qc == 'passed'):
            dir_output=('%s/%s') % (output_folder, sample_id)
            ctrl = 1
        elif(qc =='trimmed'):
            dir_output = ('%s/%s_trimmed_q%d') % (output_folder, sample_id,trimmed_quality)
            ctrl = 1

        if(ctrl == 1):
            if(not os.path.exists(dir_output)):
                try:
                    os.mkdir(dir_output)
                except OSError:
                    print ("Creation of the directory %s failed" % dir_output)
                else:
                    print ("Successfully created the directory %s " % dir_output)
            else:
                print('Directory:%s ALREADY EXIST' % dir_output)



            filename_output = '{}Aligned.sortedByCoord.out_q{}.bam'.format(sample_id,trimmed_quality)
            tmp_filename    = '{}Aligned.sortedByCoord.out.bam'.format(sample[0])

            output_samplefile = '{}/{}'.format(dir_output,sample[0])
            sample2run = ' '.join([task_program, options_template])

            #TO CHANGE!
            if(end_type =='single'):
                query=sample_update_process %(current_table, 'running',pipeline_id,sample_id, filename_input,task_id)
                if(run_dry):
                    print(query)
                else:
                    pg_conn.execute(query)
                options_change  = '--readFilesIn {input} --outFileNamePrefix {output}'.format(input=samplefile,output=output_samplefile)
            else:
                dir2check = ('%s/%s') % (dir_input,sample_id)
                query2run = query_select_paired %(current_table, pipeline_id,task_id,sample_id)
                samples_paired = pg_conn.execute(query2run).fetchall()
                filenames = [ ]
                for ff in samples_paired:
                    filenames.append(os.path.join(dir2check, ff[0]))
                    query=sample_update_process %(current_table, 'running',pipeline_id,sample_id, ff[0],task_id)
                    if(run_dry):
                        print(query)
                    else:
                        pg_conn.execute(query)

                options_change  = '--readFilesIn {input1},{input2} --outFileNamePrefix {output}'.format(input1=filenames[0],input2=filenames[1],output=output_samplefile)

            sample2run=' '.join([sample2run, options_change])

            start_time = time.time()
            print(sample2run)

            if(not run_dry):
                os.system(sample2run)


            elapse=time.time() - start_time
            print(elapse)
            date=datetime.datetime.today().strftime('%Y-%m-%d')
            #CHECK OUTPUT MAKE SENSE
            if(not run_dry):
                checkfile_result=checkoutput(dir_output,sample_id)
            else:
                checkfile_result = 1

            if(checkfile_result == 1):
                src_filename='{}/{}'.format(dir_output,tmp_filename)
                dst_filename='{}/{}'.format(dir_output,filename_output)
                if(run_dry):
                    print('RENAME')
                else:
                    os.rename(src_filename,dst_filename)


                if(end_type =='single'):
                    query_update= sample_update_add_process %(current_table,'done',date,int(elapse),dir_output ,filename_output,pipeline_id,sample_id, filename_input,task_id)
                    if(run_dry):
                        print(query_update)
                    else:
                        pg_conn.execute(query_update)
                elif(end_type =='double'):
                    for ff in filenames:
                        query_update= sample_update_add_process %(current_table,'done',date,int(elapse),dir_output ,filename_output,pipeline_id,sample_id, ff,task_id)
                        if(run_dry):
                            print(query_update)
                        else:
                            pg_conn.execute(query_update)

                #ADD SAMPLE TO PICARD1
                #INSERT OR UPDATE
                run_time_next = 0
                query_insert = query_insert_next_step % (next_table, pipeline_id,next_task_id,sample_id, dir_output, filename_output,'null','null','pending', date, run_time_next, trimmed_quality)
                query_update = query_update_next_step % (next_table,dir_output, filename_output,'null','null','pending', date, run_time_next,trimmed_quality ,filename_output,pipeline_id,next_task_id,sample_id)
                if(run_dry):
                    print(query_insert)
                    print(query_update)
                else:
                    try:
                        pg_conn.execute(query_insert)
                    except Exception as e:
                        try:
                            pg_conn.execute(query_update)
                        except Exception as e:
                            print(e)
            else:

                query_update= sample_update_add_process %(current_table,'error',date,int(elapse),dir_output ,filename_output,pipeline_id,sample_id, filename_input,task_id)
                if(run_dry):
                    print(query_update)
                else:
                    pg_conn.execute(query_update)

            query_check = sample_selection % (current_table,pipeline_id, 'pending',task_id)
            samples = pg_conn.execute(query_check).fetchall()

        if(one_sample):
            break

    if(resetquery == True):
        samples_update_process = 'UPDATE  %s SET status=\'pending\' WHERE pipeline_id=%d AND task_id=%d'  %(current_table,pipeline_id,task_id)
        print(samples_update_process)
        pg_conn.execute(samples_update_process)
    #I COULD RUN A NEW TASK HERE FOR PICARD


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--project', required=True ,help=' in which project fecth samples ')
    parser.add_argument('--pipeline', required=True ,help='which pipeline to run')
    parser.add_argument('--task',required=True ,help='current task to run')
    parser.add_argument('--next_task',required=True ,help='next task to run')
    args = parser.parse_args()

    project_code    = args.project
    pipeline_id     = int(args.pipeline)
    task_id         = int(args.task)
    next_task_id    = int(args.next_task)

    pg_user             = os.environ.get('DB_USER_LYME')
    pg_password         = os.environ['DB_PASSWORD_LYME']
    pg_host             = os.environ['DB_HOST_LYME']
    pg_conn_str         = 'postgresql://' + pg_user + ':' + pg_password + '@' + pg_host + ':5432/rnaseq_manager'
    pg_conn             = create_engine(pg_conn_str, echo=False, paramstyle='format', pool_recycle=1800)

    entry_point_star(pg_conn,project_code,pipeline_id ,task_id ,next_task_id,run_dry=False,resetquery=True,one_sample=True)

