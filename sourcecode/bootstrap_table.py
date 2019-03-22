import datetime
from sqlalchemy import create_engine
import argparse
import os




def entry_point_configurestart(project_code,pipeline_id,task_id):


    configure_table_start_pipeline(project_code,pipeline_id,task_id)

def configure_table_start_pipeline(project_code,pipeline_id,task_id):

    pg_user             = os.environ.get('DB_USER_LYME')
    pg_password         = os.environ['DB_PASSWORD_LYME']
    pg_host             = os.environ['DB_HOST_LYME']
    pg_conn_str         = 'postgresql://' + pg_user + ':' + pg_password + '@' + pg_host + ':5432/rnaseq_manager'
    pg_conn             = create_engine(pg_conn_str, echo=False, paramstyle='format', pool_recycle=1800)


    table_file2read = 'file_info'
    status2fecth = 'pending'

    select_info_task    = 'SELECT table_name FROM task WHERE task_id=%d'
    query               = select_info_task % (task_id)
    table_name_query    = pg_conn.execute(query).fetchall()
    table_name          = table_name_query[0][0]

    insert_template = 'INSERT INTO %s (pipeline_id,task_id,sample_id,dir_input,filename_input,date,end_type) '\
    'VALUES (%d,%d,\'%s\',\'%s\',\'%s\',\'%s\',\'%s\')'



#FIND FILE INFO
    find_template_file = 'SELECT sample_id, filename, directory, end_type FROM %s WHERE project_code=\'%s\' AND status=\'%s\' '
    query = find_template_file % (table_file2read,project_code, status2fecth)

    results_query_file = pg_conn.execute(query).fetchall()
    for fileinfo in results_query_file:
        sample_id = fileinfo[0]
        filename  = fileinfo[1]
        dir_input = fileinfo[2]
        end_type  = fileinfo[3]
        date      = datetime.datetime.today().strftime('%Y-%m-%d')
        query     = insert_template % (table_name, pipeline_id,task_id,sample_id,dir_input, filename,date,end_type)

        try:
            pg_conn.execute(query)
        except Exception as e:
            print(('%s %s %s' )%(sample_id,dir_input,filename))



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--project',required=True ,help='provide the project name which will stored in the db and create the folder in our server')
    parser.add_argument('--pipeline',required=True ,help='provide the project name which will stored in the db and create the folder in our server')
    parser.add_argument('--task', required=True ,help='provide the project name which will stored in the db and create the folder in our server')
    args = parser.parse_args()

    project_code = args.project
    pipeline = int(args.pipeline)
    task = int(args.task)

    entry_point_configurestart(project_code,pipeline_id,task_id)
