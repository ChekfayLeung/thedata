﻿# thedata
##卫生总费用数据处理
该项目分为3个部分

1.使用Pre_merge.py将各个省上传的表格进行提取汇总

2.使用Split.py将汇总的文件（多个文件）按行政区域-机构区分

3.使用code_icd.py对数据进行编码

4.使用readmission.py计算住院间隔时长

##运行环境

###安装Python/Conda

访问 https://docs.conda.io/en/latest/miniconda.html 下载适合自己系统的安装包，安装Python

###安装运行包

在Terminal/Power Shell/命令窗口，运行

 " conda install ipython pandas numpy fuzzywuzzy openpyxl xlrd -y"
 
###运行程序

通过在命令窗口执行 "python 文件所在的位置/文件.py"运行程序

例如使用Split.py: 运行

假设thedata保存在 "C://user/Downloads/"

"python C://user/Downloads/thedata/split.py"
