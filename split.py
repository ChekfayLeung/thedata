hp_t1 = {1: '综合医院', 2: '中医医院', 3: '专科医院', 4: '妇幼保健院', 5: '专科医院', 6: '专科医院', 7: '专科医院', 8: '专科医院', 9: '专科医院',
        10: '专科医院', 11: '专科医院', 12: '专科医院', 13: '专科医院', 14: '专科医院', 15: '专科医院', 16: '专科医院', 17: '专科医院',
        18: '专科医院', 19: '专科医院', 20: '专科医院', 21: '专科医院', 22: '专科医院', 23: '基层卫生机构', 24: '基层卫生机构', 25: '门诊机构',
        26: '门诊机构', 27: '门诊机构', 28: '疾控机构', 29: '专科医院', 30: '专科疾病防治院'}

hp_t2= {1: '综合医院', 2: '中医医院', 3: '专科医院', 4: '妇幼保健院', 5: '专科医院', 6: '专科医院', 7: '专科医院', 8: '专科医院', 9: '专科医院',
        10: '专科医院', 11: '专科医院', 12: '专科医院', 13: '专科医院', 14: '专科医院', 15: '专科医院', 16: '专科医院', 17: '专科医院',
        18: '专科医院', 19: '专科医院', 20: '专科医院', 21: '专科医院', 22: '专科医院', 23: '专科医院',
        24 :'专业疾病防治机构',25 :'专业疾病防治机构',26 :'专业疾病防治机构',27 :'专业疾病防治机构',28 :'专业疾病防治机构',
        29 :'专业疾病防治机构',30 :'专业疾病防治院',31 :'专业疾病防治机构',32 :'专业疾病防治机构',33 :'专业疾病防治机构',
        34 :'专业疾病防治机构',35 :'专业疾病防治机构',36 :'专业疾病防治机构',37 :'专业疾病防治机构',38 :'专业疾病防治机构',
        39 :'专业疾病防治机构',40 :'专业疾病防治机构',41 :'专业疾病防治机构',
        42 :'基层机构',43 :'基层机构',44 :'基层机构',45 :'基层机构',46 :'基层机构',47 :'基层机构',48 :'基层机构',49 :'基层机构',
        50 :'基层机构',51 :'基层机构',52 :'基层机构',53 :'基层机构',54 :'基层机构',55 :'基层机构',56 :'基层机构',57 :'基层机构',
        58 :'基层机构',59 :'基层机构'}

def level(input):
    try:
        if float(input) > 0: return float(input)
    except:pass
    leve=list({'省':1, '市':2, '区':3, '县':4, '乡':5,"镇":5, '村':6, '街道':6}.keys())
    l=[l in input for l in leve]
    leve = {'省': 1, '市': 2, '区': 3, '县': 4, '乡': 5,"镇":5, '村': 6, '街道': 6}
    for i in range(len(l)):
        if l[len(l)-i-1]:return leve.get(list(leve.keys())[len(l)-i-1])
    return

def run(data):
    for hp_type in tqdm(pd.Categorical(data.hp_type).categories):
        for level in pd.Categorical(data.level).categories:
            for hc in pd.Categorical(data.hc).categories:
                if len(data.query("level == @level and hp_type==@hp_type and hc == @hc")) == 0: continue
                try:
                    path = "{}/{}".format(savepath, "{}{}{}_split.csv".
                                          format({1: '省级', 2: '市级', 3: '区级', 4: '县级'}.get(level),
                                                 hp_t.get(hp_type),
                                                 {1: '住院', 2: '门诊'}.get(hc)))
                    if "sda" in path:
                        cache = pd.read_stata(path)
                    elif 'csv' in path:
                        cache = pd.read_csv(path)
                    elif 'txt' in path:
                        cache = pd.read_fwf(path)
                    elif 'xls' in path:
                        cache = pd.read_xlsx(path)
                except:
                    cache = pd.DataFrame()
                cache = pd.concat([cache,
                                   data.query("level == @level and hp_type==@hp_type and hc == @hc")], sort=False
                                  ).reset_index(drop=True)  # .drop_duplicates().reset_index(drop=True)
                if len(cache) > 0:
                    cache.to_csv("{}/{}".format(savepath, "{}{}{}_split.csv".
                                                format({1: '省级', 2: '市级', 3: '区级', 4: '县级'}.get(level),
                                                       hp_t.get(hp_type),
                                                       {1: '住院', 2: '门诊'}.get(hc))), date_format="%Y/%m/%d",
                                 index=False)
                del cache


def get_hp_type(input):
    import numpy as np
    try:
        if float(input)>0: return float(input)
    except:pass
    hp_t={'医院': 1,'中心医院':1, '中医医': 2, '儿童医院': 3, '妇幼保健': 4, '传染病': 5, '精神病': 6, '口腔': 7, '眼科': 8, '肿瘤': 9,
          '心血管': 10, '骨科': 11, '妇产': 12, '耳鼻喉科': 13, '胸科': 14, '血液病': 15, '皮肤病': 16, '结核病': 17,
          '麻风病': 18, '职业病': 19, '康复': 20, '整形': 21, '美容': 22, '卫生服务中心': 23,"中心":23,"基层":23, '卫生院': 24,
          '社区服务站': 25,'社区医院': 25, '卫生室': 26, '诊所': 27,'门诊':27, '疾控机构': 28, '专科医院': 29, '病防治': 30}
    l = [x in input for x in hp_t]
    if any(l):
        for i in range(len(l)):
            if l[- i-1]:
                return hp_t.get(list(hp_t.keys())[- i-1])
    return np.nan

if __name__ == "__main__":
    import pandas as pd, numpy as np, tkinter as tk, tkinter.filedialog as tkf,warnings
    from tqdm import tqdm,tqdm_notebook,tnrange
    import os,sys,getopt
    t=0
    t1,t2=(False,False)
    warnings.filterwarnings("ignore")
    TK = tk.Tk()
    files = list(tkf.askopenfilenames())
    savepath = "%s/split/"%list(os.path.split(files[0]))[0]
    os.system("mkdir %s" % savepath)
    # if input('where to save? Somewhere else? {y/[n]}').lower().find('y')!= -1:
    #     savepath = tkf.askdirectory()
    TK.destroy()
    for file in tqdm(files):
        if "dta" in file:data=pd.read_stata(file)
        elif 'csv' in file:data=pd.read_csv(file)
        elif 'txt' in file:data=pd.read_csv(file)
        elif 'xls' in file:data=pd.read_xlsx(file)
        data=data.rename(columns={'agency_name':'hp_name'})
        try:
            data[data.eval('hp_type+hc+level').isna()]['level'] = data[data.eval('hp_type+hc+level').isna()][
                'hp_name'].apply(level)
        except:data['level']=data.hp_name.apply(level)
        if all(['hp_type' in data.columns, 'hc' in data.columns]):
            try:
                try:data.hp_type = data.hp_type.astype('float')
                except:data.hp_type = data.hp_name.astype('str').apply(get_hp_type)
                data.hc = data.hc.astype('float')
                data.level = data.level.astype('float')
                if data.hp_type.dropna().max() < 31 and t2 is not True:
                    hp_t=hp_t1
                    t1=True
                else:hp_t=hp_t2;t2=True
                t += len(data)
            except:
                print('Fail due to non numeric type in any hp_type or hc')
                raise ValueError
            # #split data
            # data[data.eval('hp_type+hc+level').isna()].reset_index(drop=True).\
            #     to_csv("%s/%s_Missing_Key_split.csv"%(savepath,os.path.split(file)[1]),date_format="%Y/%m/%d")
            os.system('clear')
            print("Splitting data:")
            run(data)
            # for hp_type in tqdm(pd.Categorical(data.hp_type).categories):
            #     for level in pd.Categorical(data.level).categories:
            #         for hc in pd.Categorical(data.hc).categories:
            #             if len(data.query("level == @level and hp_type==@hp_type and hc == @hc"))==0:continue
            #             try:
            #                 path = "{}/{}".format(savepath,"{}{}{}_split.csv".
            #                                       format({1:'省级',2:'市级',3:'区级',4:'县级'}.get(level),
            #                                              hp_t.get(hp_type),
            #                                              {1:'住院',2:'门诊'}.get(hc)))
            #                 if "sda" in path:cache = pd.read_stata(path)
            #                 elif 'csv' in path:cache = pd.read_csv(path)
            #                 elif 'txt' in path:cache = pd.read_fwf(path)
            #                 elif 'xls' in path:cache = pd.read_xlsx(path)
            #             except:cache = pd.DataFrame()
            #             cache=pd.concat([cache,
            #                        data.query("level == @level and hp_type==@hp_type and hc == @hc")],sort=False
            #                             ).reset_index(drop=True)#.drop_duplicates().reset_index(drop=True)
            #             if len(cache) >0:
            #                 cache.to_csv("{}/{}".format(savepath, "{}{}{}_split.csv".
            #                                             format({1: '省级', 2: '市级', 3: '区级', 4: '县级'}.get(level),
            #                                                    hp_t.get(hp_type),
            #                                                    {1: '住院', 2: '门诊'}.get(hc))), date_format="%Y/%m/%d",
            #                              index=False)
            #             del cache
            #validate data
            savefiles = [x for x in list(os.listdir(savepath)) if "_split" in x]
            s = len(data)
            os.system(
                "echo {} has {} pieces of data >> {}/log.txt".format(list(os.path.split(file))[-1], s, savepath))
            del data
            cache = 0
            os.system('clear')
            # print('Checking if there were any missing data:')
            # for savefile in tqdm(savefiles):
            #     piece = len(pd.read_csv("{}/{}".format(savepath,savefile)))
            #     cache += piece
            #     os.system("echo {} has {} pieces of data >> {}/log.txt".format(savefile,piece,savepath))
            # if t==cache:
            #     os.system("echo  >> {}/log.txt".format(savepath))
            # else:
            #     os.system("echo MISSING {} PIECES OF DATA, CHECK FILE [{}] >> {}/log.txt".format((t-cache),savepath,file))
            #     os.system("echo  >> {}/log.txt".format(savepath))
        else:
            print('value not in data')
            os.system('clear')
    # os.system(
    #     "echo {} has {} pieces of data >> {}/log.txt".format(list(os.path.split(file))[0], t, savepath))
    if all([t1,t2]):
        savefiles = [x for x in list(os.listdir(savepath)) if "_split" in x]
        hp_t = hp_t2 #清理基层去了专业病的问题
        for savefile in tqdm([x for x in savefiles if '专' in x]):
            data = pd.read_csv("{}/{}".format(savepath,savefile))
            os.remove('{}/{}'.format(savepath,savefile))
            run(data)
    os.system('clear')
    sys.exit(0)
