# project name
# credential
#options more or already in place


#template ./project_name/sample_id/files_fastq
#the golden MUST BE associated with the clinical data!!!

import pandas as pd
import os
import glob
import time

from sqlalchemy import create_engine
import argparse

def main(project,path,end_type,cmd=False):
    print('main')
    pg_user             = os.environ.get('DB_USER_LYME')
    pg_password         = os.environ['DB_PASSWORD_LYME']
    pg_host             = os.environ['DB_HOST_LYME']
    pg_conn_str         = 'postgresql://' + pg_user + ':' + pg_password + '@' + pg_host + ':5432/rnaseq_manager'
    pg_conn             = create_engine(pg_conn_str, echo=False, paramstyle='format', pool_recycle=1800)


    dirs= [ os.path.join(path, name) for name in os.listdir(path) if os.path.isdir(os.path.join(path, name))]
    print(dirs)

    insert_template = 'INSERT INTO file_info (sample_id, filename, directory, file_size, project_name, end_type)'\
    ' VALUES (\'%s\',\'%s\',\'%s\',%f,\'%s\',\'%s\');'

    for name_dir in dirs:
        #THIS IS LYME ORIENTED. THE OTHER MUST FOLLOW OUR
        test_dir = os.path.join(name_dir,'Raw')
        if(os.path.isdir(test_dir)):
            print(test_dir)
            for sample in os.listdir(test_dir):
                print(sample)
                filenames = os.listdir(os.path.join(test_dir, sample))
                for filename in filenames:
                    substrings = filename.split('.')
                    if(len(substrings) > 1):
                        if(substrings[-1] == 'fastq' or (substrings[-2] == 'fastq' and substrings[-1] == 'gz')):

                            full_filename = os.path.join(test_dir,sample,filename)
                            filesize = round(os.path.getsize(full_filename)/float(1024*1024*1024),2)
                            query = insert_template % (sample, filename,test_dir,filesize, project, end_type)
                            try:
                                print(filename)
                                print(query)
                                pg_conn.execute(query)
                            except Exception as e:
                                print(e)
                                print('SAMPLE ALREADY INCLUDED')
                                exit()

                            #ADD ON DB
                            #DUMMY WAY BUT I AM NOT EXPECTING HUNDREADS OF INSERT

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--project' ,required=True ,help='provide the project name which will stored in the db and create the folder in our server')
    parser.add_argument('--credential' ,help='required when add new samples')
    parser.add_argument('--path' ,help='required when add new samples')
    parser.add_argument('--end' ,help='type sequencing single or double')
    parser.add_argument('--mv', default=False, required=True,help='move file into new directory ')
    args = parser.parse_args()

    project_name = args.project
    print(args.mv)
    main(project_name,args.path,args.end ,args.mv)

