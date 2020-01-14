if __name__ =="__main__":
    from it.icd import *;from it.data_clean import *;from it import *
    import pandas as pd, numpy as np, os, sys
    import time, os, sys, requests,re
    from it.icd import resubs, fuzzit, clean_diag, has_icd
    print("编码中")
    if True:
        cwd = os.getcwd()
        data = pd.read_csv("./cache.csv")
        try:
            data.loc[data.eval("age<=1"), "disease"] = data.loc[data.eval("age<=1"), "disease"]. \
                apply(lambda x: "儿%s" % str(x) if all([len(str(x)) >= 3, len(re.findall("[儿围产]", str(x))) == 0]) else x)
        except:
            pass
        data = pd.DataFrame(data.pop("disease"))
        data.rename(columns={"disease": "疾病名称"}, inplace=True)
        data = data.loc[:, ["疾病名称"]]
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
        result[0] = result[0].rename({'诊断名称': "match"}, axis=1)
        # result[0]['match'] = result[0]['诊断名称']  # 构建变量
        data['icd'] = data['疾病名称'].copy().apply(has_icd)
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
        data['diag'] = data['疾病名称'].copy().apply(clean_diag, keep=1)
        results = pd.concat(result).reset_index().drop('index', axis=1)
        data1 = data.copy()
        data = pd.DataFrame({"diag": data.loc[:, "diag"].drop_duplicates().reset_index(drop=True)})
        result = [[]]
        print("\n第二步:数据处理，时间较长请耐性等待，程序占用较多资源，电脑可能会发生卡顿等情况\n")
        if len(data) > 0:  # 模拟匹配
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
        else:
            Threads = []
        result = pd.merge(data1, pd.concat(result[1:], sort=True), how="left")
        result = pd.concat([result, results], sort=True)
        result = result.drop_duplicates('疾病名称').reset_index(drop=True)
        result.to_csv("./cache.csv")
        del result, Threads