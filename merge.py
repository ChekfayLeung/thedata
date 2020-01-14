"""中心检查数据库用"""
def afile():
    f = open("{}\\{}log.txt".format(dire,re.split("[\\\/\.]",filepath)[-2]), "a+")
    f.write("{}\n".format(filepath))
    f.write("数据量：{}条\n".format(len(data)))
    f.write("存在问题的数据:{}条\n".format(data.eval("CHECK!=''").sum()))
    for var in ["疾病", "费用", "age", "sex", "totalexp", "reimburse", "oop", "account", "医院", "fs2"]:
        f.write("{}错误：{}条\n".format(var, data['CHECK'].apply(FIND, sub=var).sum()))
    f.write("----------------------------------\n\n")
    f.close()

def main():
    import pandas as pd, tkinter.filedialog as tkf, sys, os, re, numpy as np, tkinter as tk, multiprocessing
    from it.data_clean import *
    from it import *
    cwd = os.getcwd()
    f = open("{}\log.txt".format(cwd), 'w+')
    f.close()
    TK = tk.Tk()
    path = tkf.askdirectory()
    filepaths = getfile(path, keywords=input("识别字段？="))
    # filepath = tkf.askopenfilenames() # Load Data
    # if isinstance(filepath, tuple):
    #     filepaths = list(filepath)
    # else:
    #     filepaths = [filepath[0]]
    # filepaths=[x for x in filepaths if x.find(".xls")!=-1]
    TK.destroy()
    # province=input("province=")
    if True:
        for filepath in filepaths:
            # os.system("""start %s""" % filepath)
            # hos=input("{}\nContinue? OR hp_name=".format(filepath))
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
                    print("\r{}".format(i))
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
                print("进入保存阶段，请勿停止")
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
                print("存在问题的数据:{}条".format(data.eval("CHECK!=''").count()))
                dire = "\\".join(re.split("[\\/]", filepath)[:-1])
                afile()
                f = open("{}\\log.txt".format(cwd), 'a+')
                f.write(open("{}\\{}log.txt""".format(dire, re.split("[\\\/\.]", filepath)[-2]), "r").read())
                f.close()
                del data
    if True:
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
        for file in files:
            data = pd.concat([data, pd.read_csv(file)], sort=False)
        data.reset_index(drop=True, inplace=True)
        print("编码中")
        if True:
            import time, os, sys, requests
            from it.icd import resubs, fuzzit, clean_diag, has_icd
            cwd = os.getcwd()
            data.reset_index(drop=True).to_csv("{}/cache.csv".format(cwd), date_format='%Y/%m/%d', index=False)
            del data
            os.system("python code_icd.py")
            result = pd.read_csv("./cache.csv")
            os.system("rm ./cache.csv")
            for file in files:
                data = pd.read_csv(file)
                data = pd.merge(data, result[['疾病名称', 'match', 'icd']], how="left", left_on="disease",
                                right_on="疾病名称")  # 还要将icd的也搬出来
                data = pd.concat([data[data['icd'].notna()],
                                  pd.merge(data[data['icd'].isna()],  # 将没有icd的补全icd
                                           pd.read_json("{}/it/icd.json".format(cwd)).dropna(axis='rows',
                                                                                             subset=['icd'])
                                           , how='left', left_on="match", right_on='诊断名称') \
                                 # .drop(["诊断名称_y","icd_x"], axis=1)
                                 # .rename({'诊断名称_x': '诊断名称',"icd_y":"icd"}, axis=1)])\
                                 .drop(["诊断名称", "icd_x"], axis=1)
                                 .rename({"icd_y": "icd"}, axis=1)]) \
                    .reset_index(drop=True)
                data.loc[:, 'icd'].fillna("NOT_MATCHED", inplace=True)
                data.loc[
                    data['icd10'].apply(lambda x: str(x).upper()[:3]) != data['icd'].apply(
                        lambda x: str(x).upper()[:3]),
                    "CHECK"] += "icd编码错误"
                data.to_csv(re.sub("\.csv", ".csv", file), date_format="%Y/%m/%d", index=False)
            print("REPORT:\n完整性测试:", end='')
            data = pd.DataFrame()
            for file in files:
                data = pd.concat([data, pd.read_csv(re.sub("\.csv", ".csv", file))], sort=False)
            print("MATCHED:{2}\nABSOLUTE_ACCURACY:{0}\nNOT_MATCHED:{1}\nBLANK:{3}" \
                  # .format(round(len(data.query("诊断名称==match")) / len(data), 2), \
                  .format(round(len(data.query("疾病名称==match")) / len(data), 2), \
                          round(len(data.query("icd=='NOT_MATCHED'"))),
                          round(len(data.query("icd!='NOT_MATCHED'")) / len(data), 2)
                          , len(data[data['icd'].isna()])))
            if True:
                f = open("{}\\log.txt".format(path), "a+")
                f.write("{}\n".format(path))
                f.write("合并库数据量：{}条\n".format(len(data)))
                f.write("合并库存在问题的数据:{}条\n".format(data.eval("CHECK!=''").sum()))
                for var in ["疾病", "费用", "age", "sex", "totalexp", "reimburse", "oop", "account", "医院", "fs2", "icd"]:
                    f.write("{}错误：{}条\n".format(var, data['CHECK'].apply(FIND, sub=var).sum()))
                f.write("----------------------------------\n\n")
                f.close()
        data.reset_index(drop=True).to_csv("{}/result.csv".format(path), date_format='%Y/%m/%d', index=False)
    os.system("start {}".format(path))
    os.system("start {}\\log.txt".format(cwd))
    sys.exit(1)

if __name__ == "__main__":
    main()
