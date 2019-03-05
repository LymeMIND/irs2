
from sqlalchemy import create_engine
import os
import time
import datetime
import pandas as pd
import glob, shutil
import argparse


#TO REMOVE PATH_FASTQC
def step_trimm(path_fastqc,pg_conn, pipeline_id,task_id,next_task_id,run_dry=True,resetquery=True,one_sample=True):




#to check
    select_option_features_count ='SELECT option_name,option_value FROM option INNER JOIN task ON option.task_id = task.task_id WHERE task.task_id=%d;'

    query= select_option_features_count % (task_id)
    options=pg_conn.execute(query).fetchall()
    options_test={c.option_name:c.option_value for c in options}
    options_template =' '.join(option+' ' + options_test[option] for option in options_test)


##queries
    sample_select_sample        = 'SELECT Distinct sample_id, dir_input,filename_input,q '\
                          'FROM %s '\
                          'WHERE pipeline_id=%d AND status=\'%s\' AND task_id=\'%s\' ORDER BY sample_id LIMIT 1'

    sample_update_process       = 'UPDATE %s SET status=\'%s\' '\
                                'WHERE  pipeline_id=%d AND sample_id=\'%s\' AND filename_input=\'%s\' AND task_id=%d'
    sample_update_step_process  = 'UPDATE %s SET status=\'%s\', date=\'%s\' , run_time=\'%s\',dir_output=\'%s\', filename_output=\'%s\' '\
                                  'WHERE  pipeline_id=%d AND sample_id=\'%s\' AND filename_input=\'%s\' AND task_id=%d AND q=%d'



#TO CHANGE file_process_star
    query_insert_star = 'INSERT INTO file_process_star (pipeline_id,task_id,sample_id,dir_input,filename_input,dir_output,filename_output,status,date, run_time,qc,end_type,trimmed_quality ) '\
    'VALUES(%d,%d,\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',%d,\'%s\', \'%s\',%d)'
    query_update_star = 'UPDATE  file_process_star SET dir_input=\'%s\', filename_input=\'%s\', dir_output=\'%s\', filename_output=\'%s\','\
            ' status=\'%s\' ,date=\'%s\',run_time=\'%s\', qc=\'%s\',trimmed_quality=%d  WHERE pipeline_id=%d AND task_id=%d AND sample_id=\'%s\' AND filename_input=\'%s\''


    query_task  = 'SELECT path,command, output_directory,table_name FROM task WHERE task_id=%d' % (task_id);
    print(query_task)
    results     = pg_conn.execute(query_task).fetchall()

    output_folder   = results[0][2]
    table_name      = results[0][3]
    task_program    = '{path_value}/{command}'.format(path_value=results[0][0],command=results[0][1])


    query = sample_select_sample %(table_name,pipeline_id, 'pending',task_id)
    samples = pg_conn.execute(query).fetchall()

    fastqc_path ='{}/FASTQC_trimmed_{}'.format(path_fastqc, str(options_test['--quality']))

    try:
        print(fastqc_path)
        os.mkdir(fastqc_path)
    # except OSError:
    except Exception as e:
        print ("Creation of the directory %s failed" % fastqc_path)
    else:
        print ("Successfully created the directory %s " % fastqc_path)

    while(len(samples) > 0):
        print(samples)
        sample      = samples[0]

        quality     = sample[3]
        dir_output      = ('%s/%s_trimmed_%d') % (output_folder, sample[0], sample[3])
        filename_input  = sample[2]

        if('--paired'in options_test):
            samplefile1 = '{dirinput}/{filename_input}'.format(dirinput=sample[1],filename_input=sample[2])
            samplefile2 = '{dirinput}/{filename_input}'.format(dirinput=sample[1],filename_input=sample[2].replace('R1','R2',2))

            replacement     = '_val_1_q%d.fq.gz' % (sample[3])
            filename2move   = sample[2].replace('.fastq.gz','_val_1.fq.gz')
            filename_output = sample[2].replace('.fastq.gz',replacement)

        else:
            samplefile1 = '{dirinput}/Raw/{sample_id}/{filename_input}'.format(dirinput=sample[1],sample_id=sample[0],filename_input=sample[2])

            filename2move   = sample[2].replace('.fastq.gz','_trimmed.fq.gz')
            replacement     = '_trimmed_q%d.fq.gz' % (sample[3])
            filename_output = sample[2].replace('.fastq.gz',replacement)

        try:
            os.mkdir(dir_output)
        except OSError:
            print ("Creation of the directory %s failed" % dir_output)
        else:
            print ("Successfully created the directory %s " % dir_output)

        query=sample_update_process % (table_name,'running',pipeline_id,sample[0], sample[2],task_id)
        if(run_dry):
            print(query)
        else:
            pg_conn.execute(query)


        dir_output_fastqc='{}/{}_trimmed_{}'.format(fastqc_path, sample[0], sample[3])

        try:
            os.mkdir(dir_output_fastqc)
        except OSError:
            print ("Creation of the directory %s failed" % dir_output_fastqc)
        else:
            print ("Successfully created the directory %s " % dir_output_fastqc)

        sample2run = ' '.join([task_program, options_template])

        #TO CHANGE
        if('--paired'in options_test):
            options_change  = '--output_dir {output_dir} {input1} {input2}'.format(output_dir=dir_output,input1=samplefile1,input2=samplefile2)
        else:
            options_change  = '--output_dir {output_dir} {input1}'.format(output_dir=dir_output,input1=samplefile1)

        sample2run=' '.join([sample2run, options_change])

        start_time = time.time()
        print(sample2run)

        if(not run_dry):
            os.system(sample2run)

        elapse=time.time() - start_time
        date = datetime.datetime.today().strftime('%Y-%m-%d')

        f_src = '{}/{}'.format(dir_output,filename2move)
        f_dst = '{}/{}'.format(dir_output,filename_output)
        os.rename(f_src,f_dst)


        if('--paired'in options_test):
            # 'R2_val_2'
            #TO CHECK
            f_src=f_src.replace('R1','R2')
            f_src=f_src.replace('val_1','val_2')
            print(f_src)
            f_dst=f_dst.replace('R1','R2')
            f_dst=f_dst.replace('val_1','val_2')
            print(f_dst)
            os.rename(f_src,f_dst)

        ####
        print('#######')
        for file in glob.glob(dir_output+'/*html'):
            try:
                shutil.move(file,dir_output_fastqc )
            except Exception as e:
                pass
        for file in glob.glob(dir_output+'/*txt'):
            try:
                shutil.move(file,dir_output_fastqc )
            except Exception as e:
                pass
        for file in glob.glob(dir_output+'/*zip'):
            try:
                shutil.move(file,dir_output_fastqc )
            except Exception as e:
                pass

#INSERT STAR
#INSERT OR UPDATE
        if('--paired'in options_test):
            query_insert = query_insert_star % (pipeline_id,next_task_id,sample[0], dir_output, filename_output,'null','null','pending', date, 0,'trimmed' ,'double', quality)
        else:
            query_insert = query_insert_star % (pipeline_id,next_task_id,sample[0], dir_output, filename_output,'null','null','pending', date, 0,'trimmed' ,'single', quality)

        query_update = query_update_star % (dir_output, filename_output,'null','null','pending', date, 0, 'trimmed',quality,pipeline_id,next_task_id,sample[0], filename_output)

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
        query_update = sample_update_step_process % (table_name,'done',date,int(elapse),dir_output,filename_output, pipeline_id, sample[0],filename_input,task_id,sample[3])
        query_check  = sample_select_sample % (table_name,pipeline_id, 'pending',task_id)
        if(run_dry):
            print(query_update)
            print(query_check)
        else:
            pg_conn.execute(query_update)
            samples = pg_conn.execute(query_check).fetchall()

        if(one_sample):
            break

    if(resetquery == True):
        samples_update_process = 'UPDATE %s SET status=\'pending\' WHERE pipeline_id=%d AND task_id=%d' %(table_name,pipeline_id,task_id)
        print(samples_update_process)
        pg_conn.execute(samples_update_process)



if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--pipeline', required=True ,help='which pipeline to run')
    parser.add_argument('--task',required=True ,help='current task to run')
    parser.add_argument('--next_task',required=True ,help='next task to run')
    #TO REMOVE
    parser.add_argument('--path_fastqc',required=True ,help='next task to run')
    args = parser.parse_args()
    pipeline_id= int(args.pipeline)
    task_id = int(args.task)
    next_task_id = int(args.next_task)

    #TO REMOVE
    path_fastqc = args.path_fastqc

    ##Database connection
    pg_user             = os.environ.get('DB_USER_LYME')
    pg_password         = os.environ['DB_PASSWORD_LYME']
    pg_host             = os.environ['DB_HOST_LYME']
    pg_conn_str         = 'postgresql://' + pg_user + ':' + pg_password + '@' + pg_host + ':5432/lymemind_rnaseq'
    pg_conn             = create_engine(pg_conn_str, echo=False, paramstyle='format', pool_recycle=1800)

    step_trimm(path_fastqc,pg_conn ,pipeline_id,task_id,next_task_id,run_dry=False,resetquery=False,one_sample=False)
