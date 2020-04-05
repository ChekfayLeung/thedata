if __name__=='__main__':
    import pandas as pd,os,time,re,sys
    from it.icd import *
    from it.data_clean import cleanit
    from it.excel_cleaning import read_excel
    cwd = os.getcwd()
    print("当前工作环境 {}".format(cwd))
    print("第一步：读取需要处理的文件")
    time.sleep(1.5)
    try:
        filepath = askfile()
        #filepath = "{}/onedrive/pycharmprojects/ds/beijing/data/test_data.xls".format(os.environ.get('USERPROFILE'))
        data = read_excel(filepath)
        data.drop_duplicates(inplace=True)
        data.reset_index(drop=True,inplace=True)
        data.to_json("{}\\cache.json".format(os.getcwd()))
        #data.dropna(subset=['诊断名称'],axis=0,inplace=True)#第一步去除空白
        data = pd.DataFrame({'诊断名称':pd.Categorical(data['诊断名称']).categories})#将诊断名称化为分类变量减少计算量
        result = [pd.merge(data,pd.read_json("{}\\icd.json".format(cwd))
                                 .dropna(axis='rows',subset=['icd'])
                                 #.reset_index().drop('index',axis=1))
                                 ,left_on="诊断名称",right_on='诊断名称',how='left')#通过合并获得能直接匹配的诊断名称
            ]#增加一个没有任何万全匹配的情况的except
        data = data[result[0]['icd'].isna()].reset_index().drop('index',axis=1)#将分类去除已经匹配的减少计算量
        result[0]=result[0][result[0]['icd'].notna()]
        result[0]['match']=result[0]['诊断名称']#构建变量
        data['icd']=data['诊断名称'].copy().apply(has_icd)#将诊断名称中有icd的提取出来作为icd
        try:
            result.append(data[data['icd'].notnull()].copy())
            result[1] = pd.merge(result[1],
                                 pd.read_json("{}\\icd.json".format(cwd))
                                 .dropna(axis='rows', subset=['icd'])
                                 # .reset_index().drop('index',axis=1),
                                 , left_on='icd', how='left', right_on='icd').rename(
                {'诊断名称_x': '诊断名称', '诊断名称_y': 'match'}, axis=1)
            result[1]['match'].fillna("诊断含icd编码",inplace=True)
        except:pass
        data = data[data['icd'].isnull()].drop('icd', axis=1)
        result=[pd.concat(result).reset_index().drop('index',axis=1)]
        data['diag'] = data['诊断名称'].copy().apply(clean_diag,keep=1)
    except:
        print("文件读取失败，请核对excel文件是否符合数据格式要求：\n\t1.表格第一行为变量名称，占用一行，没有合并或空缺等"
              "\n\t2.excel左下角sheet中仅保存了需要处理的表格，删除包括‘疫苗’等表格"
              "\n\t3.确认其他数据格式是否正确，数据格式可自行百度"
              "\n当完成以上操作规范好数据格式可重新运行程序")
        sys.exit(1)
    print("第二部数据处理，时间较长请耐性等待，程序占用较多资源，电脑可能会发生卡顿等情况\n")
    processnum = 4
    r1 = [0 + i * len(data) // processnum for i in range(processnum)]
    r2 = [(i+1) * len(data) // processnum for i in range(processnum)]
    Threads = []
    queue = multiprocessing.Queue()
    for i in range(processnum):
        p = fuzzit(data,r1[i], r2[i],queue)
        p.start()
        Threads.append(p)
    i=0
    no_alive=False
    while len(result)<(len(Threads)+1):
        i +=1
        print('\rProcessing...{0}% {1}'.format(len(result)*100//(len(Threads)+1),['\\','|','/','--'][i%4]),end='')
        try:
            result.append(queue.get(block=False))
            if any([p.is_alive() for p in Threads]):pass
            else:
                no_alive=[p.is_alive() for p in Threads]
                result.append(queue.get(block=False))
                break
        except multiprocessing.queues.Empty: pass#print('\rno data...',end='')
        time.sleep(3)
    print('\rProcessing...{0}% {1}'.format(100, ['\\', '|', '/', '--'][i % 4]), end='')
    for p in Threads:
        p.join()
    result = pd.concat(result).reset_index(drop=True)
    print("REPORT:\n完整性测试:",end='')
    data = cleanit(pd.read_json("{}\\cache.json".format(cwd)))
    os.remove("{}\\cache.json".format(cwd))
    if '疾病名称' in data.columns:
        data = data.rename({'疾病名称':'诊断名称'},axis=1)
    data = pd.merge(data,result[['诊断名称','match','icd']],how="left")#还要将icd的也搬出来
    data = pd.concat([data[data['icd'].notna()],
                      pd.merge(data[data['icd'].isna()],#将没有icd的补全icd
                               pd.read_json("{}/icd.json".format(cwd)).dropna(axis='rows',subset=['icd'])
                               , how='left', left_on="match", right_on='诊断名称')\
                     .drop(["诊断名称_y","icd_x"], axis=1).rename({'诊断名称_x': '诊断名称',"icd_y":"icd"}, axis=1)])\
        .reset_index(drop=True)
    data.loc[:,'icd'].fillna("NOT_MATCHED",inplace=True)
    print("MATCHED:{2}\nPERFECT_ACCURACY:{0}\nNOT_MATCHED:{1}\nBLANK:{3}" \
          .format(round(len(data.query("诊断名称==match")) / len(data), 2), \
                  round(len(data.query("icd=='NOT_MATCHED'"))),
                  round(len(data.query("icd!='NOT_MATCHED'")) / len(data), 2)
                  ,len(data[data['icd'].isna()])))
    #if all(no_alive) == False:
        #print(no_alive)
        #print("Error in multiprocessing, processes were dead before ended")
    print("第四步保存，建议命名为 '{}'7,请注意在文件名后加上'.xlsx'后缀" \
          .format(re.sub('\.', "_更新.", re.split("[\\/]", filepath)[-1])))
    savepath = asksave()
    if savepath[-5:] == ".xlsx" or savepath[-4:] == ".xls":
        pass
    else:
        savepath += ".xlsx"
    data.to_excel(savepath)
    sys.exit(0)
