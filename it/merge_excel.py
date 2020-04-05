def get_data(inputs,name):
    import pandas as pd, numpy as np, tkinter as tk, tkinter.ttk as ttk, tkinter.filedialog as tkf,os,sys,re
    try:
        sheets = pd.ExcelFile(inputs).sheet_names
    except:
        return pd.DataFrame()
    if not isinstance(sheets,list):sheets = [sheets]
    try:
        sheet = [pd.read_excel(inputs,sheet_name=sheet) for sheet in sheets if (name in
                                                                                pd.read_excel(inputs,sheet_name=sheet).
                                                                                columns[0]) is True][0]
    except:return pd.DataFrame()
    name = re.split("[：\s+]",sheet.iloc[0,0])[1]
    try:
        for row in sheet.index:
            try:sheet.loc[row,:].apply(int);data=sheet.loc[row,:].values
            except:continue
        return pd.DataFrame(data=[[name]+list(data)],columns=["机构名称","年平均在职职工人数（人）","卫生技术人员（人）",\
                                                                 "其他技术人员（人）","管理人员（人）","工勤技能人员（人）",\
                                                                 "在职职工年人均工资（万元/人）"])
    except:
        return pd.DataFrame()


if __name__ == "__main__":
    from tkinter import filedialog as tkf
    import tkinter as tk, tkinter.ttk as tkk,pandas as pd,os
    root = tk.Tk()
    path = tkf.askopenfilenames()
    root.destroy()
    result = pd.DataFrame()
    names ='XSW基层医疗卫生机构调查表'
    for p in list(path):
        result = pd.concat([result,get_data(p,names)])