from sqlalchemy import create_engine
import os
import time
import datetime
import pandas as pd

def merge_geneid(pipeline_id,task_id,filename_csv,one_sample=False):

##Database connection
    pg_user             = os.environ.get('DB_USER_LYME')
    pg_password         = os.environ['DB_PASSWORD_LYME']
    pg_host             = os.environ['DB_HOST_LYME']
    pg_conn_str         = 'postgresql://' + pg_user + ':' + pg_password + '@' + pg_host + ':5432/lymemind_rnaseq'
    pg_conn             = create_engine(pg_conn_str, echo=False, paramstyle='format', pool_recycle=1800)



    step        = 3
    select_files_features_count_2merge ='SELECT dir_output,sample_id FROM file_process_features_count' \
    ' WHERE pipeline_id=%d AND task_id=%d AND step=%d AND active=True ORDER BY sample_id;'
    query=select_files_features_count_2merge%(pipeline_id,task_id,step)
    samples = pg_conn.execute(query).fetchall()
    for ii,sample in enumerate(samples):

        filename='{}/{}_geneID'.format(sample[0],sample[1])
        print(sample[1])
        if(ii==0):
            df_ms6_master = pd.read_csv(filename, delimiter='\t',skiprows=[0], header=0, names=['gene_ID', 'a','b','c','d','e',str(sample[1])], index_col=0,usecols=['gene_ID',str(sample[1])])

        if(one_sample and ii ==1):
            df_ms6_sample = pd.read_csv(filename, delimiter='\t',skiprows=[0], header=0, names=['gene_ID', 'a','b','c','d','e',str(sample[1])],  index_col=0,usecols=['gene_ID',str(sample[1])])
            df_ms6_master= pd.concat([df_ms6_master,df_ms6_sample],axis=1)
            # print(df_ms6_sample)
            # input('')
            break;
        elif(ii > 0):
            df_ms6_sample = pd.read_csv(filename, delimiter='\t',skiprows=[0], header=0, names=['gene_ID', 'a','b','c','d','e',str(sample[1])],  index_col=0,usecols=['gene_ID',str(sample[1])])
            df_ms6_master= pd.concat([df_ms6_master,df_ms6_sample],axis=1)


    print(df_ms6_master.head())
    # print(df_ms6_sample)
    df_ms6_master.to_csv(filename_csv)
if __name__ == '__main__':
    filename_csv='LYME_geneID.csv'
    pipeline_id = 1
    task_id     = 6
    merge_geneid(pipeline_id,task_id,filename_csv)
