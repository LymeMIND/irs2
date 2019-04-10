import os


if __name__ == '__main__':

    path = '/data2/projects/lymeMind/data/RNAseq/PTLDS/wangy33.u.hpc.mssm.edu/TD_Data_Sending/'
    path_plate = 'TD_00704_LymeMIND_PTLDS'
    batches = os.listdir(path)
    for ii ,dir in enumerate(sorted(batches)):
        dir_level2= path+dir
        if(os.path.isdir(dir_level2)):
            # print(dir_level2)
            batches_samples = os.listdir(dir_level2+'/'+path_plate)

            for dir2 in sorted(batches_samples):
                dir_level3= dir_level2+'/'+path_plate+'/' + dir2
                if(os.path.isdir(dir_level3)):
                    samples = os.listdir(dir_level3)
                    for sample in samples:
                        print(sample)

