def testcol(data):
    if len(data.columns) >=25:return True
    else:return False

def testrow(input,type):
    try:
        type(input)
        return input
    except:return "DROPIT"


def testintn(data):
    global intg
    from numpy import isnan
    """测试数据的完整性"""
    if testcol(data) and all(x in data.columns for x in ["年龄","性别"]):
        data = data.rename({"患者编号": "病人ID",
                            "疾病名称": "诊断名称","病种名称":"诊断名称",
                            "就诊日期": '诊病时间', "入院日期":"诊病时间","入院时间":"诊病时间",
                            "门诊总费用": "总费用","住院总费用":"总费用",
                            "西药": "西药费", "中成药": "中成药费", "中草药": "中草药费",
                            "保险统筹基金支付费用": "医保统筹支付", "保险统筹基金支付": "医保统筹支付", '统筹金支付': "医保统筹支付",
                            "个人账户支付费用": "医保账户支付", "个人账户支付": "医保账户支付", "医保帐户支付": "医保账户支付",
                            "患者自付费用": "自付费用"}, axis=1)  # 用fuzzywuzzy代替
        data.rename(columns={data.columns[list(data.columns).index("药品费")+1]:"西药费",
                                  data.columns[list(data.columns).index("药品费")+2]:"中成药费",
                                  data.columns[list(data.columns).index("药品费")+3]:"中草药费"
                             #,data.columns[list(data.columns).index("参保类型")+1]:"医保统筹支付"
                             #,data.columns[list(data.columns).index("参保类型")+2]:"医保账户支付"
                             #,data.columns[list(data.columns).index("参保类型")+3]:"自付费用"
                             },inplace=True)
        dis=data.columns[list(data.columns).index("诊断名称")+2:list(data.columns).index("诊断名称")+8:2]
        for var in dis:
            t1=data.loc[:,var].apply(isinstance,args=(str,))
            t2 = data.loc[:,'诊断名称']!=data.loc[:,var]
            t=t1==t2
            data.loc[t, "诊断名称"] += ';'#分割符号
            data.loc[t,"诊断名称"]+=data.loc[t,var]
        for var in data.copy().columns:
            if "check" in var.lower():
                data.drop(var,axis=1,inplace=True)
        if "救助" in data.columns[-1]:
            data.rename(columns={
                data.columns[-4]:"医保统筹支付"
                ,data.columns[-3]:"医保账户支付"
                ,data.columns[-2]:"自付费用"
            },inplace=True)
        else:
            data.rename(columns={
                data.columns[-3]: "医保统筹支付"
                , data.columns[-2]: "医保账户支付"
                , data.columns[-1]: "自付费用"
            },inplace=True)
        try:
            data.iloc[:,0]=data.iloc[:,0].apply(testrow,type=float)
            data = data.query("患者编号!='DROPIT'").reset_index(drop=True)
            if isnan(data.loc[0,"病人ID"]):data.drop(0,axis=0,inplace=True)
        except:
            try:
                int(data.loc[0,"病人ID"])
                pass
            except:data.drop(0,axis=0,inplace=True)
        vars = data.columns
        missing = []
        for var in ['病人ID', '性别', '年龄', '出生日期', '诊断名称', '诊病时间',
                    '总费用', '药品费', '西药费', '中成药费', '中草药费', '挂号费', '诊察费', '检查费', '治疗费', '手术费', '化验费',
                    '其他费', '医保统筹支付', "医保账户支付", '自付费用']:
            if not var in vars:
                intg = False
            else:
                intg = True
        if not intg:pass
        else:
            intg=True
        return intg
    else:
        intg = False
        return intg



def read_excel(filepath):
    from it.data_clean import testint
    from pandas import read_excel,DataFrame,concat
    result = DataFrame()
    try:
        i=1
        data = read_excel(filepath,sheet_name=-i)
        while True:
            print(
                '\rProcessing...0% {}'.format( ['\\', '|', '/', '—'][i]), end='')
            if len(data.columns)<20:i+=1;data= read_excel(filepath, sheet_name=-i);continue
            if data.shape ==(0,0): i+=1;data= read_excel(filepath, sheet_name=-i);continue
            if len(data.columns)>30:i+=1;data= read_excel(filepath, sheet_name=-i);continue
            #if testcol(data) == False: i+=1;data = read_excel(filepath, sheet_name=-i);continue
            if testintn(data) == False:
                for header in range(4):
                    print('\rProcessing...0% {}'.format(['\\', '|', '/', '—'][(i+header)//4]), end = '')
                    data = read_excel(filepath,sheet_name=-i,header=header+1)
                    if testintn(data):
                        result = concat([result,testint(data)])
                        break
            else:result = concat([result,testint(data)])
            i += 1
            try:data = read_excel(filepath, sheet_name=-i)
            except:break
        return result.reset_index(drop=True)
    except IndexError:
        print("文件读取失败，请核对excel文件是否符合数据格式要求：\n\t1.表格第一行为变量名称，占用一行，没有合并或空缺等"
              "\n\t2.excel左下角sheet中仅保存了需要处理的表格，删除包括‘疫苗’等表格"
              "\n\t3.确认其他数据格式是否正确，数据格式可自行百度"
              "\n当完成以上操作规范好数据格式可重新运行程序")
        input()
        return None