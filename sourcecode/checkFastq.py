import pandas as pd
import os
import glob

def check_cat_samples(name,field,dry=True):
    # print(filename)
    path = os.getcwd()
    df_ms6_samples = pd.read_csv(name)
    count = 0
    more_lanes = []
    for ii, sample in df_ms6_samples.iterrows():
        name = sample['Orig ID']
        if(sample[field]!='None'):
            print(name)
            path2check = '../FASTQ/'+ sample[field]+'/Raw/'+sample['MSSM Core ID']
            names_found = glob.glob(path2check+'/*_L*.fastq.gz')
            if(len(names_found) > 1):
               file2save='../FASTQ/'+ sample[field]+'/Raw/'+sample['MSSM Core ID']+'/'+sample['MSSM Core ID'] +'_conc.fastq.gz'
# > file2save
               ss= 'cat %s  > %s' % (path2check+'/*_L*.fastq.gz', file2save)
               print(ss)
               if(dry==False):
                   os.system(ss) 
               more_lanes.append(1)
               #print(names_found)
               count+=1
	    else:
               more_lanes.append(0)
    print(count)

def check_samples_size(name):
    path = os.getcwd()
    df_ms6_samples = pd.read_csv(name)
    count_strange = 0
    count=0
    sample_file_size_info = []
    for ii, sample in df_ms6_samples.iterrows():
        name = sample['Orig ID']
        file_size_info= [0,0]
        for jj,field in enumerate(['Folder Name','Folder Name 2']):
            if(sample[field]!='None'):
                path2check = '../FASTQ/'+ sample[field]+'/Raw/'+sample['MSSM Core ID']
                names_found = glob.glob(path2check+'/*_L*.fastq.gz')
                if(len(names_found) > 1):
                   file2check='../FASTQ/'+ sample[field]+'/Raw/'+sample['MSSM Core ID']+'/'+sample['MSSM Core ID'] +'_conc.fastq.gz'
                   file_size=os.path.getsize(file2check)/float(1024*1024*1024)
                   count+=1
                   file_size_info[jj] = file_size
                else:
                   try:
                       count+=1
                       file_size=os.path.getsize(names_found[0])/float(1024*1024*1024)
                       file_size_info[jj] = file_size
                       if(file_size < 1):
                           #print(names_found[0])
                           #print(file_size)
                           count_strange+=1
                   except Exception as e:
                       pass
        sample_file_size_info.append(file_size_info) 
    print('##########################')
    print(count_strange/float(count))
    #print(sample_file_size_info)
    size1= [ss[0] for ss in  sample_file_size_info]
    size2= [ss[1] for ss in  sample_file_size_info]
    df_ms6_samples.insert(8,'File 1',pd.Series(size1))
    df_ms6_samples.insert(9,'File 2',pd.Series(size2))
    df_ms6_samples.to_csv('test1.csv')
    #df_ms6_samples
if __name__ == '__main__':
    filename = 'samples_RNAseq_ms6.csv'
    field = 'Folder Name 2'
    #check_samples(filename, field ,True)
    check_samples_size(filename)
