def testcol(data):
    if 30>len(data.columns) >=20:return True
    else:return False

def testintn(data):
    from numpy import isnan;
    import fuzzywuzzy.process as fp
    if testcol(data) and all(x in data.columns for x in ["年龄", "性别"]):
        data = data.rename({"病人ID": "患者编号", "诊断名称": "疾病名称", "病种名称": "疾病名称",
                            '诊病时间': "就诊日期","科室名称":"就诊科室",
                            "费别": "参保类型", "保险类型": "参保类型","门诊总费用":"总费用","住院总费用":"总费用",
                            "西药": "西药费", "中成药费": "中成药", "中草药费": "中草药",
                            "医保统筹支付": '保险统筹基金支付费用', "保险统筹基金支付": '保险统筹基金支付费用', '统筹金支付': '保险统筹基金支付费用',
                            "医保账户支付": '个人账户支付费用', "个人账户支付": '个人账户支付费用', "医保帐户支付": '个人账户支付费用',
                            "自付费用": "患者自付费用"}, axis=1)  # 用fuzzywuzzy代替
        data.rename({data.columns[list(data.columns).index("疾病名称") + 1]: "ICD-10编码",
                      data.columns[list(data.columns).index("药品费") + 1]: "西药",
                             data.columns[list(data.columns).index("药品费") + 2]: "中成药",
                             data.columns[list(data.columns).index("药品费") + 3]: "中草药"}, axis=1, inplace=True)
        for var in ["疾病名称", '保险统筹基金支付费用', '个人账户支付费用', '患者自付费用']:
            like = fp.extractOne(var[:4], data.columns)[0]
            data = data.rename({like: var}, axis=1)
        try:
            if isnan(data.loc[0, "患者编号"]): data.drop(0, axis=0, inplace=True)
        except:
            try:
                int(data.loc[0, "患者编号"])
                pass
            except:
                data.drop(0, axis=0, inplace=True)
        if "医疗救助负担" not in data.columns:data['医疗救助负担']=None
        vars = data.columns
        missing = []
        for var in ['患者编号', '年龄', '出生日期', '性别', '疾病名称', 'ICD-10编码', '就诊日期', '就诊科室',
                    '总费用', '治疗费', '药品费', '西药', '中成药', '中草药', '挂号费', '诊察费', '检查费', '手术费',
                    '化验费', '其他费', '参保类型', '保险统筹基金支付费用', '个人账户支付费用', '患者自付费用',
                    '医疗救助负担']:
            if not var in vars:
                intg = False
                missing.append(var)
            else:
                intg = True
        if not intg:
            print("data missing some columns: {}".format(missing))
        else:
            intg = True
        return intg
    else:
        return False

def testintnold(data):
    global intg
    from numpy import isnan
    import re
    """测试数据的完整性"""
    if testcol(data) and all(x in data.columns for x in ["年龄","性别"]):
        data = data.rename({"患者编号": "病人ID", "疾病名称": "诊断名称","病种名称":"诊断名称",
                            "就诊日期": '诊病时间', "入院日期":"诊病时间","入院时间":"诊病时间",
                            "门诊总费用": "总费用","住院总费用":"总费用",
                            "西药": "西药费", "中成药": "中成药费", "中草药": "中草药费",
                            "费别":"参保类型","保险类型":"参保类型",
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
        for var in data.copy().columns:
            if re.findall("[a-z]+",var.lower()) != []:
                data.drop(var,axis=1,inplace=True)
        if "救助" in data.columns[-1]:
            data.rename(columns={
                data.columns[-4]: "医保统筹支付"
                , data.columns[-3]: "医保账户支付"
                , data.columns[-2]: "自付费用"
            }, inplace=True)
        elif "参保类型" in data.columns:
            data.rename(columns={data.columns[list(data.columns).index("参保类型") + 1]: "医保统筹支付"
                , data.columns[list(data.columns).index("参保类型") + 2]: "医保账户支付"
                , data.columns[list(data.columns).index("参保类型") + 3]: "自付费用"
                                 }, inplace=True)
        else:
            data.rename(columns={
                data.columns[-3]: "医保统筹支付"
                , data.columns[-2]: "医保账户支付"
                , data.columns[-1]: "自付费用"
            }, inplace=True)
        try:
            if isnan(data.loc[0,"病人ID"]):data.drop(0,axis=0,inplace=True)
        except:
            try:
                int(data.loc[0,"病人ID"])
                pass
            except:data.drop(0,axis=0,inplace=True)
        vars = data.columns
        missing = []
        for var in ['病人ID', '性别', '年龄', '出生日期', '诊断名称', '诊病时间',"参保类型",
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
    from pandas import read_excel,DataFrame,concat,ExcelFile
    result = DataFrame()
    try:
        i=1
        data = read_excel(filepath,sheet_name=-i)
        while True:
            try:
                print(
                    '\rProcessing...0% {}'.format(['\\', '|', '/', '—'][i // 4]), end='')
                if len(data.columns) < 20: i += 1;data = read_excel(filepath, sheet_name=-i);continue
                if data.shape == (0, 0): i += 1;data = read_excel(filepath, sheet_name=-i);continue
                if len(data.columns) > 30: i += 1;data = read_excel(filepath, sheet_name=-i);continue
                # if testcol(data) == False: i+=1;data = read_excel(filepath, sheet_name=-i);continue
                if testintn(data) == False:
                    for header in range(4):
                        print('\rProcessing...0% {}'.format(['\\', '|', '/', '—'][(i + header) // 4]), end='')
                        data = read_excel(filepath, sheet_name=-i, header=header + 1)
                        if testintn(data):
                            result = concat([result, testint(data)])
                            break
                else:
                    result = concat([result, testint(data)])
                i += 1
                data = read_excel(filepath, sheet_name=-i)
            except:break
        return result.reset_index(drop=True)
    except IndexError:
        print("文件读取失败，请核对excel文件是否符合数据格式要求：\n\t1.表格第一行为变量名称，占用一行，没有合并或空缺等"
              "\n\t2.excel左下角sheet中仅保存了需要处理的门诊表格，删除包括‘疫苗’等表格"
              "\n\t3.确认其他数据格式是否正确，数据格式遵循以下变量名称："
              "\n['患者编号', '年龄', '出生日期', '性别', '疾病名称', 'ICD-10编码', '就诊日期', '就诊科室',\n" \
              "'总费用', '治疗费', '药品费', '西药', '中成药', '中草药', '挂号费', '诊察费', '检查费', '手术费',\n"
              "'化验费', '其他费', '参保类型', '保险统筹基金支付费用', '个人账户支付费用', '患者自付费用',\n"
              "'医疗救助负担']\n"
              "\n当完成以上操作规范好数据格式可重新运行程序")
        return None