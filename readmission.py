if __name__=='__main__':
    import pandas as pd, numpy as np;
    from tqdm import tqdm, tnrange, tqdm_notebook
    from it.data_clean import *;
    from multiprocessing import Process, Queue, Pool, Lock
    from it import *;from datetime import datetime

    def count_interval(hp_name):
        print(hp.index(hp_name), "-", len(hp), ':', hp_name)
        for id in tqdm(pd.Categorical(data.query('hp_name==@hp_name and read==True').id).categories):
            qu = data.query('hp_name==@hp_name and id==@id').sort_values(['inday']).reset_index()
            for i in range(len(qu)):
                try:
                    data.loc[qu['index'][i], 'read_time'] = abs(qu.inday[i] - qu.inday[i - 1])
                except:
                    data.loc[qu['index'][i], 'read_time'] = np.nan

    try:data = pd.read_csv("/d/data/merge_coded.csv")
    except: data = pd.read_csv("d://data/merge_coded.csv")
    # data['inday']=data.inday.apply(standarizedate)
    data.dropna(subset=['inday', 'age', 'id', 'hp_name', 'hc'])
    data = data[data.id.notna()]
    for var in ['birthday', 'inday', 'outday']:
        data[var] = pd.to_datetime(data[var], errors='coerce', yearfirst=False, format="%Y/%m/%d")
    data.inday2 = data.inday
    data['inday'] = data.inday.apply(lambda x: pd.Timestamp.to_julian_date(x) if isinstance(x, datetime) else np.nan)
    data['age'] = data.age.apply(age_cut)
    data.sort_values(['inday'], inplace=True)
    data['read'] = data.duplicated(['id', 'age', 'hp_name'])
    data['read_time'] = 0
    hp = list(pd.Categorical(data.hp_name).categories)
    with Pool(3) as pool:
        pool.map(count_interval,hp)

    # for hp_name in hp:
    #     p = Count_interval(hp_name)
    #     p.start()
    #     ps.append(p)
        # print(hp.index(hp_name), "-", len(hp), ':', hp_name)
        # ps=[]
        # for id in tqdm(pd.Categorical(data.query('hp_name==@hp_name and read==True').id).categories):
        #     qu = data.query('hp_name==@hp_name and id==@id').sort_values(['inday']).reset_index()
        #     for i in range(len(qu)):
        #         try:
        #             data.loc[qu['index'][i], 'read_time'] = abs(qu.inday[i] - qu.inday[i - 1])
        #         except:
        #             data.loc[qu['index'][i], 'read_time'] = np.nan
    # for p in tqdm(ps): p.join()
