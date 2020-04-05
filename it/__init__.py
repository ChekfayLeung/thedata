def getfile(path,filetype=".xls",keywords=''):
    import os, sys, re
    if os.path.isfile(path):return [path]
    a = []
    for i in os.listdir(path):
        if os.path.isfile("{}/{}".format(path, i)):
            if i.find(filetype) != -1 and any([any([i.find(x)!= -1 for x in list(keywords)]),keywords=='']):
                a.append("{}/{}".format(path, i))
            else:
                pass
        elif os.path.isdir("{}/{}".format(path, i)):
            for x in getfile("{}/{}".format(path, i), filetype, keywords):
                a.append(x)
    return a

def getfile(path,filetype=".xls",keywords=''):
    import os, sys, re
    if os.path.isfile(path):return [path]
    a = []
    for i in os.listdir(path):
        if os.path.isfile("{}/{}".format(path, i)):
            if i.find(filetype) != -1 and any([any([i.find(x)!= -1 for x in list(keywords)]),keywords=='']):
                a.append("{}/{}".format(path, i))
            else:
                pass
        elif os.path.isdir("{}/{}".format(path, i)):
            for x in getfile("{}/{}".format(path, i), filetype, keywords):
                a.append(x)
    return a

def runicdsearch(disease):
    def htmletree(url):
        """this method use etree"""
        import requests
        from fake_useragent import UserAgent
        import time
        from lxml import etree
        ua = UserAgent(verify_ssl=False)
        headers = {'User-Agent': ua.random}
        page = requests.get(url, headers=headers)
        if page.status_code == 403:
            i = 0
            while i < 50:
                headers = {'User-Agent': ua.random}
                page = requests.get(url, headers=headers)
                i += 1
                if i % 5 == 0:
                    time.sleep(2.2)
                if page.status_code == 200:
                    break
        if page.status_code == 200:
            # time.sleep(0.3)
            return etree.HTML(page.text)
    import requests, lxml, numpy as np
    from time import sleep
    table=0
    if isinstance(disease,list):disease=disease[0]
    while table==0:
        print('\r%s' % [list("/-\\|")][np.random.randint(0,3)],end='')
        sleep(0.05)
        htmls = htmletree("https://www.medsci.cn/sci/icd-10.do?action=search&q={}&classtype=新农合ICD10标准编码".
                          format(disease))
        if htmls is None:continue
        table=len(htmls.xpath('//*[@id="no-more-tables"]/table'))
        results = htmls.xpath("//*[@id='no-more-tables']/table/tr/td/a/text()")
        htmls = htmletree("https://www.medsci.cn/sci/icd-10.do?action=search&q={}&classtype=ICD-10疾病编码".
                          format(disease))
        if htmls is None: continue
        table += len(htmls.xpath('//*[@id="no-more-tables"]/table'))
        results +=htmls.xpath("//*[@id='no-more-tables']/table/tr/td/a/text()")
    rdisease = results[::4]
    ricd=results[1::4]
    rtype=range(len(ricd))#\
        #htmls.xpath("//*[@id='no-more-tables']/table/tr/td/text()")[2::3]
    dist = {}
    [dist.update({x:y}) for x,y,z in zip(rdisease,ricd,rtype)
     #if any([z.find("10")!=-1,z.find("GB")!=-1])
        ]
    return dist.get(disease)
    #return [rdisease,ricd,rtype]


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
          '心血管': 10, '骨科': 11, '妇产': 12, '耳鼻喉科': 13, '胸科': 14, '血液病': 15, '皮肤病': 16, '结核病': 17,
          '麻风病': 18, '职业病': 19, '康复': 20, '整形外科': 21, '美容': 22, '卫生服务中心': 23, '卫生院': 24,
          '社区服务站': 25, '卫生室': 26, '诊所': 27, '疾控机构': 28, '其他专科医院': 29, '专科疾病防治院': 30}
    l = [x in input for x in hp_t]
    if any(l):
        for i in range(len(l)):
            if l[- i-1]:
                return hp_t.get(list(hp_t.keys())[- i-1])
    return

def FIND(input,sub=''):
    return str(input).find(str(sub))!=-1

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
                break
            data['hc'] = data['hc'].apply(float)
        else:
            break
    return data