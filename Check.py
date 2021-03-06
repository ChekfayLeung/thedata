"""中心检查数据库用"""
import pandas as pd, tkinter.filedialog as tkf,sys, os,re,numpy as np,tkinter as tk, multiprocessing
from it.data_clean import *
def getfile(path,filetype=".xls",keywords=''):
    import os, sys, re
    if os.path.isfile(path):return [path]
    a = []
    for i in os.listdir(path):
        if os.path.isfile("{}/{}".format(path, i)):
            if i.find(filetype) != -1 and i.find(keywords)!= -1:
                a.append("{}/{}".format(path, i))
            else:
                pass
        elif os.path.isdir("{}/{}".format(path, i)):
            for x in getfile("{}/{}".format(path, i), filetype, keywords):
                a.append(x)
    return a


def afile():
    f = open("{}\\{}log.txt".format(dire,re.split("[\\\/\.]",filepath)[-2]), "a+")
    f.write("{}\n".format(filepath))
    f.write("数据量：{}条\n".format(len(data)))
    f.write("存在问题的数据:{}条\n".format(data.eval("CHECK!=''").sum()))
    for var in ["疾病", "费用", "age", "sex", "totalexp", "reimburse", "oop", "account", "医院", "fs2"]:
        f.write("{}错误：{}条\n".format(var, data['CHECK'].apply(FIND, sub=var).sum()))
    f.write("----------------------------------\n\n")
    f.close()

def icdcode(data):
    import time,os,sys
    from it.icd import resubs,fuzzit,clean_diag,has_icd
    if True:  # 疾病匹配步骤
        cwd=os.getcwd()
        data.to_json("{}/cache.json".format(cwd))
        data.rename(columns={"disease":"疾病名称"},inplace=True)
        data = data.loc[:,["疾病名称"]]
        data.dropna(subset=['疾病名称'], axis=0, inplace=True)  # 第一步去除空白
        data['疾病名称'] = data['疾病名称'].apply(resubs)  # 清理没有的情况
        data = pd.DataFrame({'疾病名称': pd.Categorical(data['疾病名称']).categories})  # 将诊断名称化为分类变量减少计算量
        result = [pd.merge(data,
                           pd.read_json("{}\\it\\icd.json".format(cwd)).dropna(axis='rows', subset=['icd'])
                           # .reset_index().drop('index',axis=1))
                           , left_on="疾病名称", right_on='诊断名称', how='left')
                  # left_on="诊断名称", right_on='诊断名称', how='left')  # 通过合并获得能直接匹配的诊断名称
                  ]  # 增加一个没有任何万全匹配的情况的except
        data = data[result[0]['icd'].isna()].reset_index().drop('index', axis=1)  # 将分类去除已经匹配的减少计算量
        result[0] = result[0][result[0]['icd'].notna()]
        result[0].rename({'诊断名称': "match"}, axis=1, inplace=True)
        # result[0]['match'] = result[0]['诊断名称']  # 构建变量
        data['icd'] = data['疾病名称'].copy().apply(has_icd)  # 将诊断名称中有icd的提取出来作为icd
        try:
            result.append(data[data['icd'].notnull()].copy())
            result[1] = pd.merge(result[1],
                                 pd.read_json("{}\\it\\icd.json".format(cwd))
                                 .dropna(axis='rows', subset=['icd'])
                                 # .reset_index().drop('index',axis=1),
                                 , left_on='icd', how='left', right_on='icd').rename(
                {'诊断名称': 'match'}, axis=1)
            # {'诊断名称_x': '诊断名称', '诊断名称_y': 'match'}, axis=1)
            result[1]['match'].fillna("诊断含icd编码", inplace=True)
        except:
            pass
        data = data[data['icd'].isnull()].drop('icd', axis=1)
        result = [pd.concat(result).reset_index().drop('index', axis=1)]
        data['diag'] = data['疾病名称'].copy().apply(clean_diag, keep=1)
        # data['diag'] = data['诊断名称'].copy().apply(clean_diag, keep=1)
        data = data[data['diag'].copy().apply(type) == list].reset_index(drop=True)
        print("\n第二步:数据处理，时间较长请耐性等待，程序占用较多资源，电脑可能会发生卡顿等情况\n")
        processnum = 4
        r1 = [0 + i * len(data) // processnum for i in range(processnum)]
        r2 = [(i + 1) * len(data) // processnum for i in range(processnum)]
        Threads = []
        queue = multiprocessing.Queue()
        for i in range(processnum):
            p = fuzzit(data, r1[i], r2[i], queue)
            p.start()
            Threads.append(p)
        i = 0
        no_alive = False
        while len(result) < (len(Threads) + 1):
            i += 1
            print(
                '\rProcessing...{0}% {1}'.format(len(result) * 100 // (len(Threads) + 1), ['\\', '|', '/', '—'][i % 4]),
                end='')
            try:
                result.append(queue.get(block=False))
                if any([p.is_alive() for p in Threads]):
                    pass
                else:
                    no_alive = [p.is_alive() for p in Threads]
                    result.append(queue.get(block=False))
                    break
            except multiprocessing.queues.Empty:
                pass  # print('\rno data...',end='')
            time.sleep(3)
        print('\rProcessing...{0}% {1}'.format(100, ['\\', '|', '/', '—'][i % 4]), end='')
        for p in Threads:
            p.join()
        result = pd.concat(result).reset_index(drop=True)
        print("REPORT:\n完整性测试:", end='')
        data = pd.read_json("{}\\cache.json".format(cwd))
        os.remove("{}\\cache.json".format(cwd))
        # if '疾病名称' in data.columns:
        #    data = data.rename({'疾病名称':'诊断名称'},axis=1)
        # data = pd.merge(data,result[['诊断名称','match','icd']],how="left")#还要将icd的也搬出来
        data = pd.merge(data, result[['疾病名称', 'match', 'icd']], how="left")  # 还要将icd的也搬出来
        data = pd.concat([data[data['icd'].notna()],
                          pd.merge(data[data['icd'].isna()],  # 将没有icd的补全icd
                                   pd.read_json("{}/it/icd.json".format(cwd)).dropna(axis='rows', subset=['icd'])
                                   , how='left', left_on="match", right_on='诊断名称') \
                         # .drop(["诊断名称_y","icd_x"], axis=1)
                         # .rename({'诊断名称_x': '诊断名称',"icd_y":"icd"}, axis=1)])\
                         .drop(["诊断名称", "icd_x"], axis=1)
                         .rename({"icd_y": "icd"}, axis=1)]) \
            .reset_index(drop=True)
        data.loc[:, 'icd'].fillna("NOT_MATCHED", inplace=True)
        print("MATCHED:{2}\nABSOLUTE_ACCURACY:{0}\nNOT_MATCHED:{1}\nBLANK:{3}" \
              # .format(round(len(data.query("诊断名称==match")) / len(data), 2), \
              .format(round(len(data.query("疾病名称==match")) / len(data), 2), \
                      round(len(data.query("icd=='NOT_MATCHED'"))),
                      round(len(data.query("icd!='NOT_MATCHED'")) / len(data), 2)
                      , len(data[data['icd'].isna()])))
        # if all(no_alive) == False:
        # print(no_alive)
        # print("Error in multiprocessing, processes were dead before ended")
        del result, Threads
        return data

def standarizenum(input):
    import re, numpy as np
    try:return float(re.sub("[,，\s]",'',input))
    except:
        try:return float(input)
        except:return np.nan


def generator(events=[],counts=[]):
    import numpy.random as random,numpy as np
    pools=np.array([x*y for x,y in zip(events,counts)])
    np.random.randint(len(pools))

def checkpositive(input,vars=""):
    try:
        input=float(input)
        if input>=0:
            return ''
        else:return "{}错误;\n".format(vars)
    except:return "{}错误;\n".format(vars)


def rename(data):
    return data.rename(columns={"age":"年龄","sex":"性别","disease":"疾病名称","icd10":"ICD-10编码","fs":"参保类型",
                         "totalexp":"总费用","birthday":"出生日期"}
                       )
def renameT(data):
    return data.rename(columns={"age":"年龄","sex":"性别","disease":"疾病名称","icd10":"ICD-10编码","fs":"参保类型",
                         "totalexp":"总费用","birthday":"出生日期"}
                       )

def getadress(input,region="全国"):
    import requests
    ak="egghXODe4IvPHNp0dqjvBYGEDIyEqFa5"
    try:
        data = requests.get("http://api.map.baidu.com/geocoding/v3/?address={}&output=json&ak={}&callback=showLocation //GET请求"
            .format(input,ak)
                            )
        pos=data.json().get("result").get("location")
        data = requests.get("http://api.map.baidu.com/reverse_geocoding/v3/?ak={}&output=json"
                            "&language=zh-CN&coordtype=wgs84ll&location={},{}  //GET请求"
                            .format(ak,pos.get("lat"),pos.get("lng")))
        return data.json().get("result").get('addressComponent')
    except:pass


def checktype(input,types,vars):
    if isinstance(input,types):return ""
    else:
        try:types(input);return ""
        except:return "{}错误;\n".format(vars)

def hp_type(input):
    try:
        if float(input)>0: return float(input)
    except:pass
    hp_t={'综合': 1, '中医': 2, '儿童医院': 3, '妇幼保健': 4, '传染病': 5, '精神病': 6, '口腔': 7, '眼科': 8, '肿瘤': 9,
          '心血管': 10, '骨科': 11, '妇产（科）': 12, '耳鼻喉科': 13, '胸科': 14, '血液病': 15, '皮肤病': 16, '结核病': 17,
          '麻风病': 18, '职业病': 19, '康复': 20, '整形外科': 21, '美容': 22, '卫生服务中心': 23, '卫生院': 24,
          '社区服务站': 25, '卫生室': 26, '诊所': 27, '疾控机构': 28, '其他专科医院': 29, '专科疾病防治院': 30}
    l = [x in input for x in hp_t]
    if any(l):
        for i in range(len(l)):
            if l[- i-1]:
                return hp_t.get(list(hp_t.keys())[- i-1])
    return

def FIND(input,sub=''):
    return input.find(sub)!=-1

def level(input):
    try:
        if float(input) > 0: return float(input)
    except:pass
    leve={'省':1, '市':2, '区':3, '县':4, '乡':5, '村':6, '街道':6}
    l=[l in input for l in leve]
    for i in range(len(l)):
        if l[len(l)-i-1]:return leve.get(list(leve.keys())[len(l)-i-1])
    return


def findloc(data):
    while True:
        if any(data[  # 写入医院
                   ['level', 'province', 'city', 'county',
                    'hp_name', 'hc', 'pu-private', 'hp_type']
               ].isna().apply(any)):
            try:
                if any(data["hp_name"].notna()):
                    hos = data.loc[data["hp_name"].notna(), "hp_name"].values[0]
                else:
                    hos = re.split("、", re.sub("[\d.调查表副本]", "", re.split("[\\\/\.]", filepath)[-2]))[0]
                print(filepath)
                if all(data['hp_name'].isna()): data['hp_name'] = data['hp_name'].fillna(hos)
                if all(data['level'].isna()): data['level'] = level(hos)
                if all(data['hp_type'].isna()): data['hp_type'] = hp_type(hos)
                if all(data['level'].isna()):
                    t = input("{}\nlevel=".format(filepath))
                    if t != '': data.loc[data['level'].isna(), "level"] = level(t)
                if all(data['hp_type'].isna()):
                    t = input("\rhp_type=")
                    if t != '': data.loc[data['hp_type'].isna(), "hp_type"] = hp_type(t)
                if all(data["pu-private"].isna()):
                    t = input("\rpu-private=")
                    if t != "": data.loc[data["pu-private"].isna(), "pu-private"] = int(t)
                if all(data["hc"].isna()):
                    data.loc[data[['outday', 'stayday', 'disease_2', 'icd10_2', 'disease_3', 'icd10_3', 'disease_4',
                                   'icd10_4', 'bedfee', 'diagnose']].notna().T.apply(any), "hc"] = 1
                    data.loc[data[['reg', 'consultation']].notna().T.apply(any), "hc"] = 2
                    t = input("\rhc=")
                    if t != '': data.loc[data['hc'].isna(), "hc"] = int(t)
                if any(data[['province', 'city', "county"]].isna().apply(all)):
                    data['province'] = province
                    pos = getadress("{}{}".format(province, hos))
                    data['province'] = pos.get("province")
                    data['city'] = pos.get("city")
                    data['county'] = pos.get("district")
                    for var in ['city', "county"]:
                        if all(data[var].isna()):
                            t = input("\r{}=".format(var))
                            if t != "": data[var] = t
                values = {}
                for v in ['level', 'province', 'city', 'county', 'hp_name', 'pu-private', 'hp_type', "hc"]:
                    value = pd.Categorical(data.loc[data[v].notna(), v]).categories
                    if len(value) > 0:
                        value = value[0]
                    else:
                        value = np.nan
                    values.update({v: value})
                data = data.fillna(value=values)  # 用不是空的填入
                data.loc[data[
                             ['level', 'province', 'city', 'county',
                              'hp_name', 'hc', 'pu-private', 'hp_type']
                         ].isna().T.apply(any), "CHECK"] += "医院信息部分/全部缺失;\n"
                break
            except KeyboardInterrupt:
                raise
            except:
                print("医院错误")
                continue
            data['hc'] = data['hc'].apply(float)
        else:
            break
    return data

if __name__ == "__main__":
    cwd = os.getcwd()
    f = open("{}\log.txt".format(cwd), 'w+')
    f.close()
    TK= tk.Tk()
    filepaths = getfile(tkf.askdirectory(),keywords=input("识别字段？="))
    #filepath = tkf.askopenfilenames() # Load Data
    # if isinstance(filepath, tuple):
    #     filepaths = list(filepath)
    # else:
    #     filepaths = [filepath[0]]
    TK.destroy()
    filepaths=[x for x in filepaths if x.find(".xls")!=-1]
    #province=input("province=")
    for filepath in filepaths:
        os.system("""start %s""" % filepath)
        hos=input("{}\nContinue? OR hp_name=".format(filepath))
        data = pd.DataFrame(columns=
                            ['id', 'age', 'sex', "birthday", 'inday', 'outday', 'stayday', 'dept1', 'dept2',
                             'disease',
                             'icd10',
                             'disease_2', 'icd10_2',
                             'disease_3', 'icd10_3', 'disease_4', 'icd10_4', 'totalexp', 'treatexp', 'drug',
                             'drug_w',
                             'drug_chn1', 'drug_chn2',
                             'bedfee', 'reg', 'consultation', 'diagnose', 'check', 'surgey', 'test', 'nurse',
                             'other',
                             'fs', 'fs1','fs2', 'reimburse', 'account', 'oop',
                             'ms', 'hp_type', 'level', 'source', 'province', 'city', 'county', 'hp_name', 'hc',
                             'pu-private',
                             "CHECK"])
        i = 1
        while True:
            try:
                print("\r{}".format(i))
                cache = pd.read_excel(filepath, sheet_name=-i)
                if "sex" in cache.columns:
                    data = pd.concat([data, cache], sort=False)
                i += 1
            except:
                del cache
                break
        data[["年龄", "性别", "出生日期"]] = data[['age', 'sex', 'birthday']]
        if "pu_private" in data.columns: data["pu-private"] = data['pu_private']
        if "icd_10" in data.columns: data['icd10'] = data['icd-10']
        if "surgry" in data.columns: data['surgey'] = data["surgry"]
        if "durg" in data.columns:data["drug"]=data["durg"]
        data["CHECK"] = ''
        if hos !='': data["hp_name"]=hos
        for var in ["totalexp", 'treatexp', 'drug', 'drug_w', 'drug_chn1', 'drug_chn2',
                     'bedfee', 'reg', 'consultation', 'diagnose', 'check', 'surgey', 'test',
                     'nurse', 'other', 'reimburse', 'account', 'oop']:
            data[var]=data[var].apply(standarizenum)
        test1=data[['totalexp',"reimburse","oop","account"]].fillna(0). \
            eval("totalexp!=reimburse+account+oop")
        test2 = data[["totalexp", 'treatexp', 'drug', 'drug_w', 'drug_chn1', 'drug_chn2',
                     'bedfee', 'reg', 'consultation', 'diagnose', 'check', 'surgey', 'test',
                     'nurse', 'other']].fillna(0).\
            eval("totalexp!="
                 "treatexp+drug++bedfee+reg+consultation+"
                 "diagnose+check+surgey+test+nurse+other")
        test = test1 + test2
        data.loc[test, "CHECK"] += "费用错误;\n"
        # data.loc[test,'totalexp'] = pd.DataFrame([
        #     data[[ 'treatexp', 'drug','bedfee', 'reg', 'consultation', 'diagnose', 'check', 'surgey', 'test',
        #              'nurse', 'other']].fillna(0).T.sum().values,
        #     data[["reimburse","oop","account"]].fillna(0).T.sum().values]).T.max(axis=1)
        for var in ['totalexp', 'drug', 'reimburse', "account", "oop"]:
            data.loc[test, "CHECK"] += data.loc[test, var].apply(checkpositive, vars=var)
        del test
        # data=rename(data)
        data.loc[data[['disease', "icd10"]].isna().T.apply(any), "CHECK"] += "疾病/ICD缺失;\n"
        get_age(data)
        if True:  # 在这一步之前要将名字换回来 if necessary
            data["CHECK"] += data[["fs1","fs2"]].sum(axis=1).apply(checkpositive, vars="fs")
            for var in ['age', 'sex']:
                data["CHECK"] += data[var].apply(checkpositive, vars=var)
            data['CHECK'] += data['age'].apply(checktype, types=float, vars="age")  # 检查年龄
            standarizegender(data)  # 整性别
            if len(pd.Categorical(data['sex']).categories) > 2:
                fix = data.query("sex!='1' and sex !='2'")
                pass  # 写按照比例填入性别的代码
        data = findloc(data)
        data.sort_values(['CHECK'], ascending=False, na_position="last", inplace=True)
        data = data[::-1].reset_index(drop=True)
        if True:  # 保存步
            print("进入保存阶段，请勿停止")
            if any(data.eval("hc==2")):
                try:
                    data.loc[data.eval("hc==2"), ["CHECK", 'id', 'age', 'sex', 'disease', 'icd10', 'inday', 'dept1',
                                                  'totalexp',
                                                  'treatexp', 'drug', 'drug_w',
                                                  'drug_chn1', 'drug_chn2', 'reg', 'consultation', 'check', 'surgey',
                                                  'test',
                                                  'other', 'fs',"fs1", 'fs2', 'reimburse',
                                                  'account', 'oop', 'ms', 'hp_type', 'level', 'source', 'province',
                                                  'city',
                                                  'county', 'hp_name', 'hc',
                                                  'pu-private']]. \
                        to_excel(re.sub("\.xls\w?", "_门诊.xlsx", filepath))
                except:
                    data.loc[data.eval("hc==2"), ["CHECK", 'id', 'age', 'sex', 'disease', 'icd10', 'inday', 'dept1',
                                                  'totalexp',
                                                  'treatexp', 'drug', 'drug_w',
                                                  'drug_chn1', 'drug_chn2', 'reg', 'consultation', 'check', 'surgey',
                                                  'test',
                                                  'other', 'fs',"fs1", 'fs2', 'reimburse',
                                                  'account', 'oop', 'ms', 'hp_type', 'level', 'source', 'province',
                                                  'city',
                                                  'county', 'hp_name', 'hc',
                                                  'pu-private']]. \
                        to_excel(re.sub("\.xls\w?", "_门诊.json", filepath))
            if any(data.eval("hc==1")):
                try:
                    data.loc[data.eval("hc==1"),
                             ["CHECK", 'id', 'age', 'sex', 'inday', 'outday', 'stayday', 'dept1', 'dept2', 'disease',
                              'icd10', 'disease_2', 'icd10_2',
                              'disease_3', 'icd10_3', 'disease_4', 'icd10_4', 'totalexp', 'treatexp', 'drug', 'drug_w',
                              'drug_chn1',
                              'drug_chn2', 'bedfee', 'diagnose', 'check', 'surgey', 'test', 'other', 'fs', "fs1",'fs2',
                              'reimburse', 'account',
                              'oop', 'ms', 'hp_type', 'level', 'source', 'province', 'city', 'county', 'hp_name', 'hc',
                              'pu-private']]. \
                        to_excel(re.sub("\.xls\w?", "_住院.xlsx", filepath))
                except:
                    data.loc[data.eval("hc==1"),
                             ["CHECK", 'id', 'age', 'sex', 'inday', 'outday', 'stayday', 'dept1', 'dept2', 'disease',
                              'icd10', 'disease_2', 'icd10_2',
                              'disease_3', 'icd10_3', 'disease_4', 'icd10_4', 'totalexp', 'treatexp', 'drug', 'drug_w',
                              'drug_chn1',
                              'drug_chn2', 'bedfee', 'diagnose', 'check', 'surgey', 'test', 'other', 'fs', "fs1",'fs2',
                              'reimburse', 'account',
                              'oop', 'ms', 'hp_type', 'level', 'source', 'province', 'city', 'county', 'hp_name', 'hc',
                              'pu-private']]. \
                        to_excel(re.sub("\.xls\w?", "_住院.json", filepath))
            if any(data.eval("hc!=1 and hc!=2")):
                try:
                    data[data.eval("hc!=1 and hc!=2")].to_excel(re.sub("\.xls\w?", "_没hc.xlsx", filepath))
                except:
                    data[data.eval("hc!=1 and hc!=2")].to_excel(re.sub("\.xls\w?", "_没hc.json", filepath))
            print("存在问题的数据:{}条".format(data.eval("CHECK!=''").count()))
            dire = "\\".join(re.split("[\\/]", filepath)[:-1])
            afile()
            f=open("{}\\log.txt".format(cwd),'a+')
            f.write(open("{}\\{}log.txt""".format(dire, re.split("[\\\/\.]", filepath)[-2]),"r").read())
            f.close()
            del data
    os.system("start {}".format(dire))
    os.system("start {}\\log.txt".format(cwd))
