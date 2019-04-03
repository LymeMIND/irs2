import pandas as pd
import os
import glob
import time

from sqlalchemy import create_engine
import argparse

def update_database_file(project,path_samples,end_type,server_path,cmd=False):
    pg_user             = os.environ.get('DB_USER_LYME')
    pg_password         = os.environ['DB_PASSWORD_LYME']
    pg_host             = os.environ['DB_HOST_LYME']
    pg_conn_str         = 'postgresql://' + pg_user + ':' + pg_password + '@' + pg_host + ':5432/rnaseq_manager'
    pg_conn             = create_engine(pg_conn_str, echo=False, paramstyle='format', pool_recycle=1800)


    ###
    #MAGIC STRING IT SHOULDN'T BE HERE
    ##

    server_path = '/la-forge/data2_projects/'

    dirs= [ os.path.join(path_samples, name) for name in os.listdir(path_samples) if os.path.isdir(os.path.join(path_samples, name))]


    insert_template = 'INSERT INTO file_info (sample_id, filename, directory, file_size, project_code, end_type)'\
    ' VALUES (\'%s\',\'%s\',\'%s\',%f,\'%s\',\'%s\');'


    dirprefix= '/{}/'.format(path_samples.replace(server_path,''))
    print(dirprefix)
    print(server_path)
    print(path_samples)
    if(os.path.isdir(path_samples)):
        for sample in os.listdir(path_samples):
            filenames = [filename for filename in os.listdir(os.path.join(path_samples, sample)) if(not os.path.isdir(os.path.join(path_samples,sample,filename)) and
            (filename.split('.')[-1] == 'fastq' or (filename.split('.')[-2] == 'fastq' and filename.split('.')[-1] == 'gz')))]

            #check if exist concat
            filenames_tmp = []
            for filename in filenames:
                if(filename.find('conc')==-1):
                    filenames_tmp.append(filename)
                else:
                    filenames_tmp= [filename]
                    break
            # print(len(filenames_tmp))
            for filename in filenames_tmp:
                if(not os.path.isdir(os.path.join(path_samples,sample,filename))):
                    substrings = filename.split('.')
                    if(len(substrings) > 1):
                        if(substrings[-1] == 'fastq' or (substrings[-2] == 'fastq' and substrings[-1] == 'gz')):

                            full_filename = os.path.join(path_samples,sample,filename)
                            filesize = round(os.path.getsize(full_filename)/float(1024*1024*1024),2)
                            query = insert_template % (sample, filename,dirprefix,filesize, project, end_type)
                            try:
                                pg_conn.execute(query)
                            except Exception as e:
                                print('SAMPLE ALREADY INCLUDED')
                                return -1
                                # exit()

    return 1

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--project' ,required=True ,help='provide the project name which will stored in the db and create the folder in our server')
    # parser.add_argument('--credential' ,help='required when add new samples')
    parser.add_argument('--path' ,help='required when add new samples')
    parser.add_argument('--end' ,help='type sequencing single or double')
    # parser.add_argument('--mv', default=False, required=True,help='move file into new directory ')
    args = parser.parse_args()

    project_name = args.project
    # print(args.mv)
    update_database_file(project_name,args.path,args.end)

