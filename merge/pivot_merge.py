import pandas as pd
import pickle as pkl
import partial_unzip
import glob
import time

def pivot_merge(user, skip, iterations, quarter):
        '''
        Merges the T4 and application features of the identified user
        Parameters: 
        user (string) : Id of the target user
        skip (integer): Determined by analysing the application data 
        iterations (integer) : Number of iterations 
        quarter (string) : Year and Quarter (example: 2016_Q1 )
        '''
    
        keys = pkl.load(open('../sherlock_columns.pkl','rb'))
        t4 = pd.read_csv('../t4_user' + user + '.csv')
        chunk_index = 1
        chunk_size = 5000000

        # Number of iterations is determined after analysing the user data
        # varies with each user
        for i in range(iterations):  
            print("Iteration : ",i)
            start = time.time()
            app_loc = "../" + quarter + "/Application.zip"
            partial_unzip.unzip(app_loc, nrows = chunk_size, skiprows=skip)
            app_data = pd.read_csv("../2016_Q1/Application.tsv",
                                   sep = '\t',
                                   names = keys['application'],
                                   encoding = 'latin-1')
            
            app_data = app_data[app_data['userid'] == user]
            app_data = app_data.pivot_table(index = 'uuid', columns = 'applicationname')
            app_data["uuid"] = app_data.index
            app_data.reset_index(drop=True,inplace=True)
            
            rename = []
            for tple in app_data.columns:
                if tple[0] == 'uuid':
                    rename.append('uuid')
                else:
                    rename.append(tple[0]+"_"+tple[1])
            app_data.columns = rename
            
            print("Read application :" ,i)
            merged_df = pd.merge(t4, app_data, how = 'inner', left_on = 'uuid', right_on = 'uuid')
            print("Merge ",i,"complete : shape", merged_df.shape)
            merged_df.to_csv('../'+ user + '/Testing{}.csv'.format(chunk_index)) 
            end = time.time()
            print("Time taken : ", end -start)
            chunk_index +=1
            skip += chunk_size
            print ("*"*10)

        # Merging the csv
        path = '../'+ user + '/'
        combined_csv = pd.concat([pd.read_csv(f) for f in glob.iglob(path + "*.csv")])
        combined_csv.to_csv('../'+ user + '.csv',index=False, encoding='utf-8-sig')