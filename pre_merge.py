import pandas as pd, tkinter.filedialog as tkf, sys, os, re, numpy as np, tkinter as tk, multiprocessing
from it.data_clean import *
from it import *;from multiprocessing import Pool
from warnings import showwarning;import warnings
warnings.filterwarnings("ignore")

def afile():
    f = open("{}\\{}log.txt".format(dire,re.split("[\\\/\.]",filepath)[-2]), "a+")
    f.write("{}\n".format(filepath))
    f.write("数据量：{}条\n".format(len(data)))
    f.write("存在问题的数据:{}条\n".format(data.eval("CHECK!=''").sum()))
    for var in ["疾病", "费用", "age", "sex", "totalexp", "reimburse", "oop", "account", "医院", "fs2"]:
        f.write("{}错误：{}条\n".format(var, data['CHECK'].apply(FIND, sub=var).sum()))
    f.write("----------------------------------\n\n")
    f.close()

def read_file(filepath):
    cwd = os.getcwd()
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
                         'fs', 'fs1', 'fs2', 'reimburse', 'account', 'oop',
                         'ms', 'hp_type', 'level', 'source', 'province', 'city', 'county', 'hp_name', 'hc',
                         'pu-private',
                         "CHECK", "from"])
    i = 1
    while True:
        try:
            # print("\r{} - {}".format(i,filepath),end='')
            cache = pd.read_excel(filepath, sheet_name=-i)
            if "sex" in cache.columns:
                data = pd.concat([data, cache], sort=False)
            i += 1
        except:
            del cache
            break
    if True:
        for bird in [x for x in data.columns if
                     any([all([y in x for y in ['b', 'day']]), "出生日期" in x, "Birt" in x])]:
            id = data['birthday'].isna()
            data.loc[id, 'birthday'] = data.loc[id, bird]
            if bird != "birthday": data.drop(bird, inplace=True, axis=1)
        for stay in [x for x in data.columns if any([all([y in x for y in ['st', 'day']]), "实际住院天数" in x])]:
            id = data['stayday'].isna()
            data.loc[id, 'stayday'] = data.loc[id, stay]
            if stay != "stayday": data.drop(stay, inplace=True, axis=1)
        for age in [x for x in data.columns if any([all([y in x for y in ['age']]), "年龄" in x])]:
            id = data['age'].isna()
            data.loc[id, 'age'] = data.loc[id, age]
            if age != "age": data.drop(age, inplace=True, axis=1)
        for htype in [x for x in data.columns if any([all([y in x for y in ['h', "ame"]]), "医院名称" in x])]:
            id = data['hp_name'].isna()
            data.loc[id, 'hp_name'] = data.loc[id, htype]
            if htype != "hp_name": data.drop(htype, inplace=True, axis=1)
        for hc in [x for x in data.columns if all([all([y in x for y in ['h']]), len(x) <= 3])]:
            id = data['hc'].isna()
            data.loc[id, 'hc'] = data.loc[id, hc]
            if hc != "hc": data.drop(hc, inplace=True, axis=1)
    data['from'] = str(filepath)
    data[["年龄", "性别", "出生日期"]] = data[['age', 'sex', 'birthday']].copy()
    if "pu_private" in data.columns: data["pu-private"] = data['pu_private']
    if "icd_10" in data.columns: data['icd10'] = data['icd-10']
    if "surgry" in data.columns: data['surgey'] = data["surgry"]
    if "durg" in data.columns: data["drug"] = data["durg"]
    data["CHECK"] = ''
    # if hos !='': data["hp_name"]=hos
    for var in ["totalexp", 'treatexp', 'drug', 'drug_w', 'drug_chn1', 'drug_chn2',
                'bedfee', 'reg', 'consultation', 'diagnose', 'check', 'surgey', 'test',
                'nurse', 'other', 'reimburse', 'account', 'oop']:
        data[var] = data[var].apply(standarizenum)
    test1 = data[['totalexp', "reimburse", "oop", "account"]].fillna(0). \
        eval("totalexp!=reimburse+account+oop")
    test2 = data[["totalexp", 'treatexp', 'drug', 'drug_w', 'drug_chn1', 'drug_chn2',
                  'bedfee', 'reg', 'consultation', 'diagnose', 'check', 'surgey', 'test',
                  'nurse', 'other']].fillna(0). \
        eval("totalexp!="
             "treatexp+drug+bedfee+reg+consultation+"
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
    try:
        get_age(data)
    except:
        pass
    if True:  # 在这一步之前要将名字换回来 if necessary
        data["CHECK"] += data[["fs1", "fs2"]].sum(axis=1).apply(checkpositive, vars="fs")
        standarizegender(data)  # 整性别
        for var in ['age', 'sex']:
            data["CHECK"] += data[var].apply(checkpositive, vars=var)
        data['CHECK'] += data['age'].apply(checktype, types=float, vars="age")  # 检查年龄
        if len(pd.Categorical(data['sex']).categories) > 2:
            fix = data.query("sex!='1' and sex !='2'")
            pass  # 写按照比例填入性别的代码
    # data = findloc(data)
    data.sort_values(['CHECK'], ascending=False, na_position="last", inplace=True)
    data = data[::-1].reset_index(drop=True)
    if True:  # 保存步
        # print("\r进入保存阶段，请勿停止",end=' ')
        if any(data.eval("hc==2")):
            data.loc[data.eval("hc==2"), ["CHECK", 'id', 'age', 'sex', 'birthday', 'disease', 'icd10', 'inday',
                                          'dept1',
                                          'totalexp',
                                          'treatexp', 'drug', 'drug_w',
                                          'drug_chn1', 'drug_chn2', 'reg', 'consultation', 'check', 'surgey',
                                          'test',
                                          'other', 'fs', "fs1", 'fs2', 'reimburse',
                                          'account', 'oop', 'ms', 'hp_type', 'level', 'source', 'province',
                                          'city',
                                          'county', 'hp_name', 'hc',
                                          'pu-private', 'from']]. \
                to_csv(re.sub("\.xls\w?", "_门诊.csv", filepath), date_format='%Y/%m/%d', index=False)
        if any(data.eval("hc==1")):
            data.loc[data.eval("hc==1"),
                     ["CHECK", 'id', 'age', 'sex', 'birthday', 'inday', 'outday', 'stayday', 'dept1', 'dept2',
                      'disease',
                      'icd10', 'disease_2', 'icd10_2',
                      'disease_3', 'icd10_3', 'disease_4', 'icd10_4', 'totalexp', 'treatexp', 'drug', 'drug_w',
                      'drug_chn1',
                      'drug_chn2', 'bedfee', 'diagnose', 'check', 'surgey', 'test', 'other', 'fs', "fs1", 'fs2',
                      'reimburse', 'account',
                      'oop', 'ms', 'hp_type', 'level', 'source', 'province', 'city', 'county', 'hp_name', 'hc',
                      'pu-private', 'from']]. \
                to_csv(re.sub("\.xls\w?", "_住院.csv", filepath), date_format="%Y/%m/%d", index=False)
        if any(data.eval("hc!=1 and hc!=2")):
            data[data.eval("hc!=1 and hc!=2")].to_csv(re.sub("\.xls\w?", "_没hc.csv", filepath),
                                                      date_format='%Y/%m/%d', index=False)
        # print("存在问题的数据:{}条".format(data.eval("CHECK!=''").count()),end='')
        dire = "\\".join(re.split("[\\/]", filepath)[:-1])
        # afile()
        # f = open("{}\\log.txt".format(cwd), 'a+')
        # f.write(open("{}\\{}log.txt""".format(dire, re.split("[\\\/\.]", filepath)[-2]), "r").read())
        # f.close()
        del data

def main():
    import pandas as pd, tkinter.filedialog as tkf, sys, os, re, numpy as np, tkinter as tk, multiprocessing
    from multiprocessing import Pool;from tqdm import tqdm
    cwd = os.getcwd()
    f = open("{}\log.txt".format(cwd), 'w+')
    f.close()
    TK = tk.Tk()
    path = tkf.askdirectory()
    filepaths = getfile(path, keywords='')
    TK.destroy()
    # province=input("province=")
    if True:
        with Pool(5) as p,tqdm(total=len(filepaths)) as pbar:
            for i,_ in enumerate(p.imap_unordered(read_file,filepaths)):
                pbar.update()
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
                             'fs', 'fs1', 'fs2', 'reimburse', 'account', 'oop',
                             'ms', 'hp_type', 'level', 'source', 'province', 'city', 'county', 'hp_name', 'hc',
                             'pu-private',
                             "CHECK", 'from'])
        files = getfile(path, ".csv", ["_门诊", "_住院"])
        for file in tqdm(files):
            data = pd.concat([data, pd.read_csv(file)], sort=False)
        data.reset_index(drop=True, inplace=True)
        data.to_csv("%s.csv" % path,index=False,date_format="%Y%m%d")


if __name__=="__main__":
    main()
    sys.exit(0)