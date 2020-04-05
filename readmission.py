import pandas as pd, numpy as np;from numba import vectorize
from tqdm import tqdm, tnrange, tqdm_notebook
from it.data_clean import *;
from multiprocessing import Pool
from it import *;from datetime import datetime


@vectorize(['float32(float32, float32)'], target='cuda')
def minus(qua,qub):
    return qua-qub

def run(id):
    qu = data.query('hp_name==@hp_name and id==@id').sort_values(['inday']).reset_index()
    if len(qu) > 1:
        qub = np.asarray(np.append(qu.inday, [np.nan]), dtype=np.float32)
        qua = np.asarray(np.append([np.nan], qu.inday), dtype=np.float32)
        data.loc[qu.index, 'read_time'] = np.abs(minus(qua,qub))[:len(qu)]
        del qua, qub, qu
    # for i in range(len(qu)):
    #     try:
    #          data.loc[qu['index'][i], 'read_time'] = abs(qu.inday[i] - qu.inday[i - 1])
    #     except:
    #         data.loc[qu['index'][i], 'read_time'] = np.nan

def count_interval(hp_name):
    return
    # for id in tqdm(pd.Categorical(data.query('hp_name==@hp_name and read==True').id).categories):


if __name__=='__main__':
    try:data = pd.read_csv("/D/data/read.csv")
    except:
        data = pd.read_csv("/D/data/merge_coded.csv")
        # data['inday']=data.inday.apply(standarizedate)
        data.dropna(subset=['inday', 'age', 'id', 'hp_name', 'hc'])
        data = data[data.id.notna()]
        for var in ['birthday', 'inday', 'outday']:
            data[var] = pd.to_datetime(data[var], errors='coerce', yearfirst=False, format="%Y/%m/%d")
        data['inday2'] = data.inday
        data['inday'] = data.inday.apply(
            lambda x: pd.Timestamp.to_julian_date(x) if isinstance(x, datetime) else np.nan)
        data['age'] = data.age.apply(age_cut)
        data.sort_values(['inday'], inplace=True)
        data['read'] = data.duplicated(['id', 'age', 'hp_name'])
        data['read_time'] = 0
        data['process']=0
        data = data[['inday', 'id', 'hp_name', "read", 'read_time','process']]
    hp = list(pd.Categorical(data.hp_name).categories)
    # with Pool(2) as pool:
    #     pool.map(count_interval,hp)
    for hp_name in hp:
        if any(data.query("hp_name==@hp_name and process==1").index):continue
        print(hp.index(hp_name), "-", len(hp), ':', hp_name)
        # for i in tqdm(list(pd.Categorical(data.query('hp_name==@hp_name and read==True').id).categories)):
        #     run(i)
        with Pool(5) as p:
            max_ = len(data.query("hp_name==@hp_name"))
            with tqdm(total=max_) as pbar:
                for i, _ in enumerate(p.imap_unordered(
                        run, list(pd.Categorical(data.query('hp_name==@hp_name and read==True').id).categories))):
                    pbar.update()
        data.loc[data.query("hp_name==@hp_name").index,'process']=1
        data.to_csv("/D/data/read.csv",index=False,date_format="%Y%m%d")
    data.to_csv("/D/data/merge_coded_read.csv",index=False,date_format="%Y%m%d")