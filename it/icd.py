"""tool for ICD standardize"""
import multiprocessing

def resubs(input):
    import re, numpy as np
    if isinstance(input,str):
        return re.sub('\n\t\r',';',input)
    else:return np.nan


def askfile():
    import tkinter
    root = tkinter.Tk()
    root.title("File")
    width = root['width']=600
    height=root['height']=400
    root.resizable(False,False)
    screenwidth = root.winfo_screenwidth()
    screenheight = root.winfo_screenheight()
    alignstr = '%dx%d+%d+%d' % (width, height, (screenwidth - width) / 2, (screenheight - height) / 2)
    root.geometry(alignstr)
    lb = tkinter.Text(root,width=600,height=200,wrap=tkinter.WORD)
    lb.place(x=0,y=10)
    #lb.grid(row=0,column=0)
    filenames = fileadress(lb)
    btn = tkinter.Button(root, text="Ok", command=root.destroy,width=10)
    btn.place(x=290,y=370)
    #btn.grid(row=2,column=0)
    root.mainloop()
    if len(filenames)==1:
        return filenames[0]
    else:
        return filenames


def asksave():
    import tkinter
    root = tkinter.Tk()
    root.title("Save File")
    width = root['width']=600
    height=root['height']=400
    root.resizable(False,False)
    screenwidth = root.winfo_screenwidth()
    screenheight = root.winfo_screenheight()
    alignstr = '%dx%d+%d+%d' % (width, height, (screenwidth - width) / 2, (screenheight - height) / 2)
    root.geometry(alignstr)
    lb = tkinter.Text(root,width=600,height=200,wrap=tkinter.WORD)
    lb.place(x=0,y=10)
    #lb.grid(row=0,column=0)
    filenames = saveadress(lb)
    btn = tkinter.Button(root, text="Ok", command=root.destroy,width=10)
    btn.place(x=290,y=370)
    #btn.grid(row=2,column=0)
    root.mainloop()
    if len(filenames)==1:
        return filenames[0]
    else:
        return filenames


def fileadress(lb):
    import tkinter.filedialog
    filenames = list(tkinter.filedialog.askopenfilenames())
    text = ''.join(["{};\n".format(x) for x in filenames])
    lb.insert('0.0',"You've selected:[\n{}]".format(text))
    return filenames


def saveadress(lb):
    import tkinter.filedialog
    filenames = tkinter.filedialog.asksaveasfilename()
    lb.insert('0.0',"You've selected:[\n{}]".format(filenames))
    return filenames


def has_icd(input):
    import re
    try:return re.findall(r'[A-Z]\d\d\.\w\w\w',input)[0]
    except:return


def FuzzyMatchDisease(disease):
    """give a disease name and return the best match disease standard name,return name and icd"""
    import pandas as pd, fuzzywuzzy.fuzz as fuzz, fuzzywuzzy.process as process, os
    cutoff=44
    icd = pd.read_json(
        "{}\\it\\icd.json".format(os.getcwd()))
    if isinstance(disease,float):return "NOT MATCHED"
    if isinstance(disease, list):
        result1 = pd.DataFrame(columns=['r1','sc'])
        for d in disease:
            try:
                r1, sc = process.extractBests(d, list(icd.loc[:, '诊断名称']), limit=1,
                                              score_cutoff=cutoff, scorer=fuzz.token_sort_ratio)[0]
                # result1.append(r1)
                result1 = pd.concat([result1, pd.DataFrame({'r1': [r1], 'sc': [sc]})])
            except KeyboardInterrupt:
                return
            except:
                r1 = 'NOT_MATCHED'
                sc = 0
                result1 = pd.concat([result1, pd.DataFrame({'r1': [r1], 'sc': [sc]})])
        try:return result1['r1'].values[0] #KeyError:'r1'
        except KeyError:
            print(result1)
            return "KeyError"
        except IndexError: return "NOT_MATCHED"
    elif isinstance(disease, str) and disease != '':
        icd = pd.read_json(
            "{}\\it\\cancer.json".format(os.getcwd())) if any([x in disease for x in list("癌瘤")]) else pd.read_json(
            "{}\\it\\icdnc.json".format(os.getcwd()))
        try:
            r1 = process.extractBests(disease, list(icd.loc[:, '诊断名称']), limit=1,
                                      score_cutoff=cutoff, scorer=fuzz.token_sort_ratio)[0][0]
        except KeyboardInterrupt:
            return
        except:
            r1 = 'NOT_MATCHED'
        return r1
    else:
        return 'NOT_MATCHED'


def split_case(case):
    """return turple of ICD(if exists) and list of diagnosis"""
    import re
    # get icd-10
    if isinstance(case, float):
        return ''
    try:
        case = re.sub("\s", '', case)#新增指令
        if re.search(r'\W\w\d+\.\d+', case):
            ICD = re.findall(r'\w\d+\.\d+', case.strip())[0]
        elif re.search(r'\w\w\w\d+'):
            ICD = re.findall(r'\w\w\w\d+')
        else:
            ICD = re.search(r'\w\d+', case.strip())[0]
    except:
        return ''
    case = ''.join(re.split(ICD, case.strip()))
    case = ','.join(re.split(r'\d+\.', case))
    return re.findall('\w+', case)


def clean_diag(case,keep=1):
    "接受一个字段,清理数字，字符等"
    import re
    if isinstance(case, float):
        return ''
    # elif any([x in case for x in ['烧伤','烫伤','体检','检查']])
    elif isinstance(case, str):
        if any(re.findall(r"流产\w{0,}$", str(case))): return re.sub(r"[妊娠孕\d周天,，\+？]",'',case)
        if any(re.findall(r"[头肩臀前横后侧]位\w{0,3}$|[单多双三]胎\w{0,3}$", str(case))):
            return "%s顺产" % re.findall(r"枕?[头肩臀前横后侧]位\w{0,3}$|[单多双三]胎\w{0,3}$", str(case))[0]#TODO: 看看这个枕位出不出得来
        if any(re.findall(r"[妊娠孕]+\s?\d+\w{0,2}$", str(case))): return "妊娠状态"
        if any(re.findall("[烫烧]伤",str(case))):return "多处烧伤"
        case = re.sub(r'^[a-z]{3,}', '', case)
        case = re.sub(r"慢\w?病\w?药|[CTLS腰椎胸尾]\d+",'',case)
        if case.find("待查")!=-1 and len(case)<3: return "待查"
        case = re.sub(r"°",'度',case)
        case = re.sub("[IVⅡⅢⅣ]+度|[（(].{5,}[）)]",'',case)
        case = ''.join(re.split(r"([头颈腕踝膝肘面手脚肢肾叶眼耳胸侧腹"
                                r"指趾腿臂腰椎段躯足会阴臀"
                                r"鼻窦盆]\w{0,2})[、，,\s]",case))  # TODO: 解决重复较多导致的匹配错误
        case = ';'.join(re.split(r"(\w+[炎症烧伤烫癌肿瘤折囊病术感染除癣疹损]\w{0,5})[、，,\s]",case))  # TODO: 解决三个字内疾病和方位词冲突的问题
        case = re.sub(r"第[0-9]+\s?|第[一二三四五六七八九十][、，\s]?[一二三四五六七八九十]?","",case)
        case = re.sub(r'\d+[\.。、，？！\-\\%]\d?', '', case)
        case = re.sub(r"[0-9]+[周天月年时]\s?","",case)
        case = re.sub(r"\d+类|NOS|中医：|化疗|左|右|双侧|上段|下段|职业|待查|部", '', case)
        case = re.sub(r'[\[\]()\-（）【】伴]', '', case)
        case = re.sub(r"[\s/]", ';', case)
        case = re.findall(r"\w+", case)
        if len(case) == 0: return ''
        while not re.findall(r"[a-zA-Z\u4e00-\u9fa5]+", case[0]):
            if len(case) == 1: return ''
            case = case[1:]
        # return case[:keep]
        if len(case[0]) <=1:
            return ''.join(case[:keep+2]) if len(''.join(case[:keep+2]))>3 else "%sn" % ''.join(case[:keep+2])
        return case[:keep][0] if len(''.join(case[:keep]))>3 else "%sn" % ''.join(case[:keep])
    else:
        return ''


class fuzzit(multiprocessing.Process):
    def __init__(self, data, range1, range2, queue):
        self.data = data[range1:range2]
        self.queue = queue
        multiprocessing.Process.__init__(self)

    def run(self):
        self.data.loc[:, 'match'] = self.data['diag'].apply(FuzzyMatchDisease)
        self.queue.put(self.data)
        # data.loc[self.r1:self.r2, 'match'] =data.loc[self.r1:self.r2,'diag'].apply(FuzzyMatchDisease)


