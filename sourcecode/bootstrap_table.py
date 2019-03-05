

# project name
# credential
#options more or already in place


#template ./project_name/sample_id/files_fastq
#the golden MUST BE associated with the clinical data!!!


import datetime
from sqlalchemy import create_engine
import argparse
import os

def main(project,pipeline_id,task_id):
    print('main')
    pg_user             = os.environ.get('DB_USER_LYME')
    pg_password         = os.environ['DB_PASSWORD_LYME']
    pg_host             = os.environ['DB_HOST_LYME']
    pg_conn_str         = 'postgresql://' + pg_user + ':' + pg_password + '@' + pg_host + ':5432/rnaseq_manager'
    pg_conn             = create_engine(pg_conn_str, echo=False, paramstyle='format', pool_recycle=1800)



    select_info_task    ='SELECT table_name FROM task WHERE task_id=%d'
    query   = select_info_task % (task_id)
    table_name_query = pg_conn.execute(query).fetchall()
    table_name = table_name_query[0][0]
    insert_template = 'INSERT INTO %s (pipeline_id,task_id,sample_id,dir_input,filename_input,date,end_type) '\
    'VALUES(%d,%d,\'%s\',\'%s\',\'%s\',\'%s\',\'%s\')'

    find_template_file = 'SELECT sample_id, filename, directory, end_type FROM %s WHERE project_name=\'%s\' AND status=\'%s\' '
    query = find_template_file % ('file_info',project, 'pending')
    results_query_file = pg_conn.execute(query).fetchall()
    for fileinfo in results_query_file:
        # print(fileinfo)
        sample_id =fileinfo[0]
        filename = fileinfo[1]
        dir_input = fileinfo[2]
        end_type = fileinfo[3]
        date=datetime.datetime.today().strftime('%Y-%m-%d')
        query = insert_template % (table_name, pipeline_id,task_id,sample_id,dir_input, filename,date,end_type)
        # print(query)
        try:
            pg_conn.execute(query)
        except Exception as e:
            print(('%s %s %s' )%(sample_id,dir_input,filename))



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--project' ,required=True ,help='provide the project name which will stored in the db and create the folder in our server')
    parser.add_argument('--pipeline' ,required=True ,help='provide the project name which will stored in the db and create the folder in our server')
    parser.add_argument('--task' ,required=True ,help='provide the project name which will stored in the db and create the folder in our server')

    args = parser.parse_args()

    project_name = args.project
    pipeline = int(args.pipeline)
    task = int(args.task)

    main(project_name,pipeline,task)
