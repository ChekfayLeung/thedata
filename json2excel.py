if __name__ =="__import__":
    import pandas as pd, tkinter as tk, tkinter.filedialog as tkf,os,re
    Tk=tk.Tk()
    files = list(tkf.askopenfilenames())
    for file in files:
        if file.find("json") != -1:
            try:pd.read_json(file).to_excel(re.sub("\.json",".xlsx",file))
            except:print("内存不足")
    os._exit(1)