import pandas as pd, numpy as np,sys;from numba import vectorize
from tqdm import tqdm, tnrange, tqdm_notebook;import tkinter as tk, tkinter.filedialog as tkf
from it.data_clean import *;
from multiprocessing import Pool
from it import *;from datetime import datetime


#@vectorize(['float32(float32, float32)'], target='cuda')
def minus(qua,qub):
    return qua-qub

def run(id):
    qu = data.query('hp_name==@hp_name and id==@id').sort_values(['inday']).reset_index()
    if len(qu) > 1:
        qub = np.asarray(np.append(qu.inday, [np.nan]), dtype=np.float32)
        qua = np.asarray(np.append([np.nan], qu.inday), dtype=np.float32)
        return qu['index'].values,np.abs(minus(qua,qub))[:len(qu)]
        # data.loc[qu.index, 'read_time'] = np.abs(minus(qua,qub))[:len(qu)]
        # del qua, qub, qu

def main():
    global data, hp_name
    try:data = pd.read_csv("/D/data/read.csv")
    except:
        TK= tk.Tk()
        data = pd.read_csv(tkf.askopenfilename())
        TK.destroy()
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
        data['read'] = data.duplicated(['id', 'age', 'hp_name'],keep=False)
        data['read_time'] = 0
        data['process']=0
        data = data[['inday', 'id', 'hp_name', "read", 'read_time','process']]
    hp = list(pd.Categorical(data.hp_name).categories)
    for hp_name in hp:
        if any(data.query("hp_name==@hp_name and process==1").index):continue
        print(hp.index(hp_name), "-", len(hp), ':', hp_name)
        with Pool(6) as p:
            max_ = len(data.query("hp_name==@hp_name"))//2 + 1
            with tqdm(total=max_) as pbar:
                for i, r in enumerate(p.imap_unordered(
                        run, list(pd.Categorical(data.query('hp_name==@hp_name and read==True').id).categories))):
                    try:ind,qu = r
                    except:pbar.update();continue
                    data.loc[ind,"read_time"] = qu; del ind, qu
                    pbar.update()
                data.loc[data.query("hp_name==@hp_name").index,'process']=1
                data.to_csv("/D/data/read.csv",index=False,date_format="%Y%m%d")
                pbar.update()
    data.to_csv("/D/data/merge_coded_read.csv",index=False,date_format="%Y%m%d")


if __name__=='__main__':
    main()
    sys.exit(0)