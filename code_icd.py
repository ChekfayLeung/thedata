from it.icd import *;
from it.data_clean import *;
from it import *
import pandas as pd, numpy as np, os, sys, tkinter.filedialog as tkf, tkinter as tk
import time, os, sys, requests, re, multiprocessing;
from tqdm import tqdm;
# from it.icd import resubs, fuzzit, clean_diag, has_icd, FuzzyMatchDisease

def read():
    global data,result,data1,results,signal,file,cwd
    print("编码中")
    with tqdm(total=7) as pbar:
        cwd = os.getcwd()
        try:
            data = pd.read_csv("./cache.csv")
            signal = False
        except:
            TK=tk.Tk()
            file = tkf.askopenfilename()
            TK.destroy()
            if ".csv" in file:
                data = pd.read_csv(file)
            elif ".dta" in file:data = pd.read_stata(file)
            signal = True
        pbar.update()
        try:
            data.loc[data.eval("age<=1"), "disease"] = data.loc[data.eval("age<=1"), "disease"]. \
                apply(lambda x: "儿%s" % str(x) if all([len(str(x)) >= 3, len(re.findall("[儿围产]", str(x))) == 0]) else x)
        except:
            pass
        pbar.update()
        data = pd.DataFrame(data.pop("disease"))
        data.rename(columns={"disease": "疾病名称"}, inplace=True)
        data = data.loc[:, ["疾病名称"]]
        data.dropna(subset=['疾病名称'], axis=0, inplace=True)  # 第一步去除空白
        data['疾病名称'] = data['疾病名称'].apply(resubs)  # 清理没有的情况
        data = pd.DataFrame({'疾病名称': pd.Categorical(data['疾病名称']).categories})  # 将诊断名称化为分类变量减少计算量
        pbar.update()
        result = [pd.merge(data,
                           pd.read_json("{}\\it\\icd.json".format(cwd)).dropna(axis='rows', subset=['icd'])
                           # .reset_index().drop('index',axis=1))
                           , left_on="疾病名称", right_on='诊断名称', how='left')
                  # left_on="诊断名称", right_on='诊断名称', how='left')  # 通过合并获得能直接匹配的诊断名称
                  ]  # 增加一个没有任何万全匹配的情况的except
        data = data[result[0]['icd'].isna()].reset_index().drop('index', axis=1)  # 将分类去除已经匹配的减少计算量
        result[0] = result[0][result[0]['icd'].notna()]
        result[0] = result[0].rename({'诊断名称': "match"}, axis=1)
        # result[0]['match'] = result[0]['诊断名称']  # 构建变量
        data['icd'] = data['疾病名称'].copy().apply(has_icd)
        pbar.update()
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
        pbar.update()
        data = data[data['icd'].isnull()].drop('icd', axis=1)
        data['diag'] = data['疾病名称'].copy().apply(clean_diag, keep=1)
        results = pd.concat(result).reset_index().drop('index', axis=1)
        data1 = data.copy()
        data = pd.DataFrame({"diag": data.loc[:, "diag"].drop_duplicates().reset_index(drop=True)})
        result = [[]]
        pbar.update()


def code(method="pool"):
    global data,result,results,Threads,data1
    # >>>icd Fuzzymatch<<<
    print("\n第二步:数据处理，时间较长请耐性等待，程序占用较多资源，电脑可能会发生卡顿等情况\n")
    # >>>icd Fuzzymatch<<<
    if method.lower() == "pool":
        with multiprocessing.Pool(7) as p, tqdm(len(data)) as pbar:
            for i, r in enumerate(p.imap(FuzzyMatchDisease, data.diag.values)):
                data.loc[i, 'match'] = r
                pbar.update()
        result = pd.concat([data, results], sort=True)
    # >>>try if Using Pool would be faster
    elif method.lower() == "process" and len(data) > 0:  # 模拟匹配
        processnum = 6
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
                '\rProcessing...{0}% {1}'.format(len(result) * 100 // (len(Threads) + 1),
                                                 ['\\', '|', '/', '—'][i % 4]),
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
        result = pd.merge(data1, pd.concat(result[1:], sort=True), how="left")
        result = pd.concat([result, results], sort=True)
    else:
        Threads = []
    result = result.drop_duplicates('疾病名称').reset_index(drop=True)



def save():
    if signal:
        with tqdm(total=6) as pbar:
            if ".csv" in file:
                data = pd.read_csv(file)
            elif ".dta" in file:
                data = pd.read_stata(file)
            pbar.update()
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
            pbar.update()
            data.loc[:, 'icd'].fillna("_NOT_MATCHED", inplace=True)
            try:
                if "CHECK" not in data.columns: data['CHECK'] = ''
                data.loc[data['icd10'].apply(lambda x: str(x).upper()[:3]) != data['icd'].apply(
                    lambda x: str(x).upper()[:3]),
                         "CHECK"] += "icd编码错误"
                data["CHECK"] = data.CHECK.apply(lambda x: None if x == '' else x)
            except:
                pass
            pbar.update()
            data.to_csv(re.sub("\.\w{2,}", "_code.csv", file), date_format="%Y/%m/%d", index=False)
            try:
                _ = pd.read_csv(re.sub("\.\w{2,}", ".csv", file))
            except:
                TK = tk.TK();data.to_csv(tkf.asksaveasfilename(), date_format="%Y/%m/%d", index=False);TK.destroy()
            pbar.update()
    else:result.to_csv("./cache.csv")
    del result, Threads

def main():
    read()
    code("pool")
    save()


if __name__ =="__main__":
    main()
    sys.exit(0)