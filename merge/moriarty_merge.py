import pandas as pd
import datetime
import time

def moriarty_merge(user):
    ''' 
    Merges moriarty probe details with the pivot merge for a user
    Parameters: 
        user (string) : targer user    
    '''
    combined_csv = pd.read_csv('../'+ user + '.csv')
    mor = pd.read_csv("./moriarity_probe.csv", index_col = 0)
    v1 = ['1.0']
    v3 = ['3.0', 3.0]
    mor['version'] = mor['version'].apply(lambda x: 'v1' if x in v1 else x)
    mor['version'] = mor['version'].apply(lambda x: 'v3' if x in v3 else x)
    mor_user = mor[mor['userid'] == user] 
 
    # date time conversion
    mor_user['uuid_floor'] = mor_user['uuid'].apply(lambda x: datetime.datetime.fromtimestamp(x/1000.0))    
    combined_csv['uuid_floor'] = combined_csv['uuid'].apply(lambda x: datetime.datetime.fromtimestamp(x/1000.0))

    # round to nearest 5 seconds
    mor_user['uuid_floor'] = mor_user['uuid_floor'].dt.floor(freq = '5S')
    combined_csv['uuid_floor'] = combined_csv['uuid_floor'].dt.floor(freq = '5S')
    start = time.time()
    combined_csv = pd.merge(combined_csv,
                            mor_user,
                            how = 'left',
                            left_on = ['userid','uuid_floor'],
                            right_on = ['userid','uuid_floor'])
    end = time.time()
    print(end - start)
    combined_csv.to_csv('./' + user + 'Master.csv')