import os
import glob
import time




#CREATE EVERYTHING!!!!
def fastqc(path2check):
    folders = glob.glob(path2check+'/*')
    # print(folders)
    ctrl = 0
    dirname = '/data2/projects/IPF_YALE/RNAseq/FASTQC_check'
    path_fastqc='/data1/software/FastQC_0.11.8/FastQC'
    for folder in sorted(folders):
        if(folder.split('/')[-1] == 'Sample_18b'):
            files =glob.glob(folder+'/*.gz')
            print(folder.split('/')[-1])
            ss= '{}/fastqc {} --threads 4 -o {}'.format (path_fastqc,' '.join(files),dirname )
            print(ss)
            input('press to continue')
            os.system(ss)
if __name__ == '__main__':
    fastqc(path2check='/data2/projects/IPF_YALE/RNAseq/FASTQ/raw')
