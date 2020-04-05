def testthe(data):
    """测试总费用的准确性"""
    data=data.copy()
    data.rename({"门诊总费用":"总费用","住院总费用":"总费用"},axis=1,inplace=True)
    data['费用检查'] = ''
    tnan=data.iloc[:,list(data.columns).index("总费用"):list(data.columns).index("其他费")+1].isna().T.apply(any)
    data.loc[tnan,"费用检查"]="部分项目费用未明细；"
    data.fillna(0,inplace=True)
    if "挂号费" in data.columns:
        t = data.eval("总费用 == (药品费+挂号费+诊察费+检查费+治疗费+手术费+化验费+其他费)")
    else:t = data.eval("总费用 == (药品费+床位费+诊察费+检查费+治疗费+手术费+化验费+其他费)")
    #data.loc[t == False, '费用检查'] += "项目核算错误或未明细；"
    data.loc[t ==False, '费用检查'] += "项目核算错误；"
    #t = data.eval("总费用==(医保统筹支付+医保账户支付+自付费用)")
    t = data.eval("总费用==(保险统筹基金支付费用+个人账户支付费用+患者自付费用)")
    data.loc[t == False, '费用检查'] += "统筹支付核算错误；"
    #t = data.eval("药品费!=0 and 西药费+中成药费+中草药费==0")
    #data.loc[t==True,"费用检查"]+= "药品费用未明细"
   # t = data.eval("药品费==(西药费+中成药费+中草药费)")
    t = data.eval("药品费==(西药+中成药+中草药)")
    #data.loc[t == False, '费用检查'] += "药物费用核算错误或未明细；"
    data.loc[t == False, '费用检查'] += "药物费用核算错误；"
    if not all(data.eval("费用检查==''")):
        print("费用检查:未通过")
    else:
        print("费用检查:通过")
    data.loc[data['费用检查'] == '', '费用检查'] = '检查通过'
    return data['费用检查']


def testcol(data):
    if 30>len(data.columns) >=20:return True
    else:return False

def testint(data):
    global intg
    from numpy import isnan;import fuzzywuzzy.process as fp
    if testcol(data) and all(x in data.columns for x in ["年龄", "性别"]):
        data = data.rename({"病人ID":"患者编号" , "诊断名称": "疾病名称","病种名称":"疾病名称",
                            '诊病时间':"就诊日期","科室名称":"就诊科室",
                            "费别": "参保类型", "保险类型": "参保类型","门诊总费用":"总费用","住院总费用":"总费用",
                            "西药": "西药费", "中成药费": "中成药", "中草药费": "中草药",
                            "医保统筹支付":'保险统筹基金支付费用', "保险统筹基金支付": '保险统筹基金支付费用', '统筹金支付': '保险统筹基金支付费用',
                            "医保账户支付":'个人账户支付费用', "个人账户支付":'个人账户支付费用', "医保帐户支付": '个人账户支付费用',
                            "自付费用": "患者自付费用",
                            "医疗救助负担支付":"医疗救助负担"}, axis=1)  # 用fuzzywuzzy代替
        data.rename({data.columns[list(data.columns).index("疾病名称") + 1]: "ICD-10编码",
                     data.columns[list(data.columns).index("药品费")+1]:"西药",
                                  data.columns[list(data.columns).index("药品费")+2]:"中成药",
                                  data.columns[list(data.columns).index("药品费")+3]:"中草药"},axis=1,inplace=True)
        for var in ["疾病名称", '保险统筹基金支付费用', '个人账户支付费用', '患者自付费用']:
            like = fp.extractOne(var[:4], data.columns)[0]
            data.rename({like: var}, axis=1, inplace=True)
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
            intg=True
        return data[['患者编号', '年龄', '出生日期', '性别', '疾病名称', 'ICD-10编码', '就诊日期', '就诊科室',
                     '总费用', '治疗费', '药品费', '西药', '中成药', '中草药', '挂号费', '诊察费', '检查费', '手术费',
                     '化验费', '其他费', '参保类型', '保险统筹基金支付费用', '个人账户支付费用', '患者自付费用',
                     '医疗救助负担']]
    else:intg=False;return data


def testintold(data):
    global intg
    from numpy import isnan
    import re
    """测试数据的完整性"""
    if testcol(data) and all(x in data.columns for x in ["年龄","性别"]):
        data = data.rename({"患者编号": "病人ID", "疾病名称": "诊断名称","病种名称":"诊断名称",
                            "就诊日期": '诊病时间', "入院日期":"诊病时间","入院时间":"诊病时间",
                            "门诊总费用": "总费用","住院总费用":"总费用",
                            "费别": "参保类型", "保险类型": "参保类型",
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
        for var in data.copy().columns:
            if re.findall("[a-z]+", var.lower()) != []:
                data.drop(var, axis=1, inplace=True)
        if "救助" in data.columns[-1]:
            data.rename(columns={
                data.columns[-4]:"医保统筹支付"
                ,data.columns[-3]:"医保账户支付"
                ,data.columns[-2]:"自付费用"
            },inplace=True)
        elif "参保类型" in data.columns:
            data.rename(columns={data.columns[list(data.columns).index("参保类型")+1]:"医保统筹支付"
                                 ,data.columns[list(data.columns).index("参保类型")+2]:"医保账户支付"
                                 ,data.columns[list(data.columns).index("参保类型")+3]:"自付费用"
                                 }, inplace=True)
        else:
            data.rename(columns={
                data.columns[-3]: "医保统筹支付"
                , data.columns[-2]: "医保账户支付"
                , data.columns[-1]: "自付费用"
            },inplace=True)
        try:
            if isnan(data.loc[0, "病人ID"]): data.drop(0, axis=0, inplace=True)
        except:
            try:
                int(data.loc[0, "病人ID"])
                pass
            except:
                data.drop(0, axis=0, inplace=True)
        vars = data.columns
        missing = []
        for var in ['病人ID', '性别', '年龄', '出生日期', '诊断名称', '诊病时间',"参保类型",
                    '总费用', '药品费', '西药费', '中成药费', '中草药费', '挂号费', '诊察费', '检查费', '治疗费', '手术费', '化验费',
                    '其他费', '医保统筹支付', "医保账户支付", '自付费用']:
            if not var in vars:
                intg = False
                missing.append(var)
            else:
                intg = True
        if not intg:
            print("data missing some columns: {}".format(missing))
        else:
            intg=True
            #print("Passed")
        return data[['病人ID', '性别', '年龄', '出生日期', '诊断名称', '诊病时间',"参保类型",
                    '总费用', '药品费', '西药费', '中成药费', '中草药费', '挂号费', '诊察费', '检查费', '治疗费', '手术费', '化验费',
                    '其他费', '医保统筹支付', "医保账户支付", '自付费用']]
    else:
        intg = False
        print("data missing some columns: {}".format(['病人ID', '性别', '年龄', '出生日期', '诊断名称', '诊病时间', '科室名称',
                '总费用', '药品费', '西药费', '中成药费', '中草药费', '挂号费', '诊察费', '检查费', '治疗费', '手术费', '化验费',
                '其他费', '医保统筹支付', "医保账户支付", '自付费用']))
        return data



def testicd(data):
    """测试icd是否完成"""
    icd = data['诊断代码'].isna()
    if False in icd:
        print("NaN in data")


def standarizedate(date):
    """标准化日期格式"""
    import pandas as pd, re, datetime, numpy as np
    if isinstance(date, pd.Timestamp):
        y, m, d = (date.year,date.month,date.day)
    elif isinstance(date, datetime.datetime) or isinstance(date, datetime.date):
        y, m, d = (date.year,date.month,date.day)
    elif isinstance(date, str):
        date = re.sub("\s",'',date)
        [y, m, d] = re.split(r"[/\-\\\.]", date)
    elif np.isnan(date):
        return np.nan
    else:
        try:
            birth = [x for x in re.split(r'\s', date) if x != np.nan][0]
            [y, monthdate] = re.split("年", birth)
            [m, dateraw] = re.split('月', monthdate)
            d = re.split("日", dateraw)[0]
            for i, var2 in zip(range(3), re.split(r'\w', birth)):
                [y, m, d][i] = int(var2)
        except:
            return np.nan  # no birth in data
    return datetime.date(int(y), int(m), int(d))


def date2age(birth):
    """年龄计算功能，将出生日期转换成年龄"""
    import re, numpy as np, pandas as pd, datetime
    if isinstance(birth, pd.Timestamp):
        #year, month, date = birth.isocalendar()
        #return round((year * 365 + month * 30.4167 + date) / 365.25, 1)  # 将符合日期格式的格式直接输出
        return round(birth.to_julian_date()/365,1)
    elif (isinstance(birth, datetime.datetime) or isinstance(birth, datetime.date))\
            and not isinstance(birth,pd._libs.tslibs.nattype.NaTType):
        year, month, date = (birth.year,birth.month,birth.day)
        return round((year * 365 + month * 30.4167 + date) / 365.25, 1)
    elif isinstance(birth, str):
        if any([x in birth for x in ['/','-']]):
            birth = [x for x in re.split(r'\s', birth) if x != np.nan][0]
            year = re.findall(r"\d{4}",birth)[0]
            if len(year)<=0: year=0
            return int(year)
        # if "/" in birth:
        #     birth = [x for x in re.split(r'\s', birth) if x != np.nan][0]
        #     [year, month, date] = re.split("/", birth.strip())
        #     [year, month, date] = [int(x) for x in [year, month, date]]
        #     return round((year * 365 + month * 30.4167 + date) / 365.25, 1)  # 将"1995 /11/ 10 '等日期换成天再算成年龄
        # elif "-" in birth:
        #     birth = [x for x in re.split(r'\s', birth) if x != np.nan][0]
        #     [year, month, date] = re.split("\-", birth.strip())
        #     [year, month, date] = [int(x) for x in [year, month, date]]
        #     return round((year * 365 + month * 30.4167 + date) / 365.25, 1)  # 将"1995 -11- 10 '等日期换成天再算成年龄
        else:
            try:
                birth = re.sub('[\s+零又个]', '', birth)
                year = any([x in birth for x in ["年", "岁"]])
                month = "月" in birth
                date = any([x in birth for x in ['天' or "日"]])
                if year:
                    [year, monthdate] = re.split("[年岁]", birth)
                    year = float(re.findall(r'\d+',year)[0])
                else:
                    year = 0;
                    monthdate = birth
                if month:
                    [month, dateraw] = re.split('月', monthdate)
                    month = float(re.findall(r'\d+',month)[0])
                else:
                    month = 0
                    dateraw = monthdate
                if date:
                    date = re.split("[日天]", dateraw)[0]
                    date= float(re.findall(r'\d+',date)[0])
                else:
                    date = 0
                [year, month, date] = [float(x) for x in [year, month, date]]
                return round((year * 365 + month * 30.4167 + date) / 365.25, 1)  # 将'1995年11月10日换成年龄
            except:
                try: return float(birth)
                except: return np.nan
    elif isinstance(birth, int) or isinstance(birth, float):
        return birth if not np.isnan(birth) else np.nan
    else:
        return np.nan

def age_cut(input):
    import numpy as np,re
    if "周岁" == str(input)[:-2]:
        return float(''.join(re.findall(r'\d+',str(input)))) if any(re.findall(r'\d+',str(input))) else np.nan
    elif "岁" == str(input)[-1]:
        return float(''.join(re.findall(r'\d+',str(input)))) if any(re.findall(r'\d+',str(input))) else np.nan
    elif any([x in str(input) for x in ["天", '年', '月', "岁", "/", "-"]]):
        return float(date2age(input))
    else:
        try:return float(input)
        except:return np.nan


def get_age(data):
    """标准化年龄格式"""
    if "年龄" in list(data.columns):
        data['age'] = data["年龄"].copy()
        data['age'] = data['age'].apply(age_cut)
        try:
            data["age"]=data["age"].apply(int)
        except:
            try:
                data["age"] = data["age"].apply(float)
            except:pass
        if data["age"].dtype == "int":
            pass
        elif data['age'].dtype =='float':
            if all(data['age'].notna()):pass
            else:
                if "出生日期" in list(data.columns):
                    for diagnosis_date in ["入院日期", "就诊日期", "诊病日期", "诊病时间","inday"]:  # 将所有日期变成julian_date
                        if diagnosis_date in list(data.columns):
                            break
                    if data["出生日期"].dtype == "int":
                        data.loc[data['age'].isna(),'age'] =\
                            data.loc[data['age'].isna(),diagnosis_date].copy().apply(date2age) - data.loc[data['age'].isna(),"出生日期"].copy()
                    else:
                        data.loc[data['age'].isna(),'age'] = \
                            data.loc[data['age'].isna(),diagnosis_date].copy().apply(date2age) - data.loc[data['age'].isna(),"出生日期"].copy().apply(
                            date2age)  # 日期-日期
                else:
                    data['age'] = data['age']
    # elif "出生日期" in list(data.columns):
    #     for diagnosis_date in ["入院日期", "就诊日期", "诊病日期", "诊病时间","inday"]:  # 将所有日期变成julian_date
    #         if diagnosis_date in list(data.columns):
    #             break
    #     if data["出生日期"].dtype == "int":
    #         data.loc[data['age'].isna(),'age'] = \
    #             data.loc[data['age'].isna(),diagnosis_date].copy().apply(date2age) - data.loc[data['age'].isna(),"出生日期"].copy()
    #     else:
    #         data.loc[data['age'].isna(),'age'] = \
    #             data.loc[data['age'].isna(),diagnosis_date].copy().apply(date2age) - data.loc[data['age'].isna(),"出生日期"].copy().apply(date2age)  # 日期-日期
    else:
        data['age'] = data['age']
    try:
        data['age'] = data['age'].astype("float")
        print("转化成功")
    except:
        print('转化失败')



def get_in(data):
    """标准化年龄格式"""
    if "住院天数" in list(data.columns):
        data['inpatient'] = data["住院天数"].copy()
        if data["inpatient"].dtype == "int64":
            pass
        else:
            data['inpatient']=data['inpatient'].apply(age_cut)
    elif "入院日期" in list(data.columns):
        for diagnosis_date in ["入院日期", "就诊日期", "诊病日期", "诊病时间"]:  # 将所有日期变成julian_date
            if diagnosis_date in list(data.columns):
                break
        data['inpatient'] = data[diagnosis_date].copy().apply(date2age) - data["出院日期"].copy().apply(date2age)  # 日期-日期
    else:
        data['inpatient'] = "there aren't any data available for age calculation"
    try:
        data['inpatient'] = data['inpatient'].astype("float")
        print("转化成功")
    except ValueError:
        print('转化失败')


def test_gender(data):
    import pandas as pd
    return len(pd.Categorical(data['性别']).categories) == 2


def standarizegender(data):
    import pandas as pd
    data['sex']=data['性别'].apply(str)
    cats = pd.Categorical(data['sex']).categories
    male = ["2.0","2"]
    female = ["1.0","1"]
    for cat in cats:
        if '男' in cat:
            male.append(cat)
        elif '女' in cat:
            female.append(cat)
    for m in male:
        data.loc[data.eval("sex==@m"),"sex"] = "2"
    for f in female:
        data.loc[data.eval("sex==@f"),"sex"] = "1"
    data.loc[data.eval("sex!='1' and sex!='2'"), "sex"] = '不支持的数据格式'
    if test_gender(data):
        print('性别:标准化成功')
    else:
        print("性别:标准化失败")

def insurance(input):
    import numpy as np
    try:
        return int(float(input))
    except:pass
    if isinstance(input,str):
        if any(["职工" in input,"退休" in input]): return 1
        elif "城乡" in input:
            return 10
        elif "居民" in input: return 2
        elif "农" in input: return 3
        elif "自" in input: return 8
        elif "工伤" in input: return 7
        elif "公费" in input: return 9
        elif "商业" in input: return 4
        elif any(["扶" in input, "救助" in input]):return 5
        elif "生" in input: return 6
        else:return 19
    else:return input


def cleanit(data):
    data = testint(data)
    print("通过")
    if intg == True:
        data['费用核算']=testthe(data)  # 检查费用核算是否准确
    else:
        return print("数据完成/格式出错")
    try:
        data['就诊日期'] = data['就诊日期'].apply(standarizedate)
        print("就诊日期: 标准化成功")
    except:print("就诊日期: 标准化出错")
    try:
        data['出生日期'] = data['出生日期'].apply(standarizedate)
        print("出生日期: 标准化成功")
    except:print("出生日期: 标准化失败")
    try:
        data['住院日期']=data['住院日期'].apply(standarizedate)
        data['出院日期']=data['出院日期'].apply(standarizedate)
        get_in(data)
    except:pass
    try:data['insurance']=data['参保类型'].apply(insurance);print("参保类型：提取成功")
    except:print("参保类型：提取失败")
    print("年龄：",end="")
    get_age(data)
    standarizegender(data)
    for var, name in zip(['age', 'gender', '参保类型'], ['年龄', '性别', '参保类型']):
        if any(data[var].isna()): print("{}:存在缺失数据".format(name))
    return data



