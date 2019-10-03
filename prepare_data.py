import glob, os

for data in ['data1a', 'data2a', 'data3a']:
    for f in ['training', 'validation']:
        base_path = 'car-damage-dataset/'+data+'/'+f
        lst_path = base_path+'_lst'
        os.system('mkdir '+lst_path)
        f = open(lst_path+'/label.lst', 'w')
        folders = glob.glob(base_path+'/*')
        idx = 0
        #for i, folder in enumerate(folders):
            files = glob.glob(folder+'/*.jpg')
            for file in files:
                path = folder.split('/')[-1]+'/'+file.split('/')[-1]
                #print(idx, i, path)
                f.write("%d\t%d\t%s\n"%(idx, i, path))
                idx += 1
                #new_file = file.split('.')[0]+'.jpeg'
                #os.system("mv "+file+' '+new_file)
        f.close()
           
                 
