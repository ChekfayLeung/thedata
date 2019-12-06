import sys

from cx_Freeze import setup, Executable

options = {'build_exe':{"packages":['os',"pandas",'numpy','fuzzywuzzy','time','datetime','tkinter','re','multiprocessing',
                                 "xlrd"],
                        "includes":["numpy","pandas","fuzzywuzzy","multiprocessing"]
                     ,"include_files":["c:/users/leung/onedrive/pycharmprojects/ds/thedata/icd.json"]#,\
                                       #os.path.join(sys.base_prefix, 'DLLs', 'sqlite3.dll')]
                     ,"excludes":['sqlite3']}}
base = None
if sys.platform == "win32":
    base="Win32GUI"

setup(name="ICD_CODING",
      version="0.1",
      options = options,
      description="Auto coding icd10 and standarize data format",
      executables=[Executable("C:/Users/Leung/Onedrive/Pycharmprojects/DS/thedata/__init__.py",base=base)])