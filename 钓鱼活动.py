# -*- coding: utf-8 -*-
"""
Created on Mon May  7 21:54:59 2018

@author: Efun
"""

import MySQLdb as mdb
#  pandas as pd
import os


os.chdir( r"D:\Work\201805\20180508 - 日志\log")
## 函数: 遍历文件夹下所有对象
def loop_folder( path):
    ## 遍历文件夹
    filename_list = []
    for i in os.listdir( path):
        filename_list.append( i)
    return( filename_list)


## 创建空 Dataframe
result = pd.DataFrame( columns = 
                      ['EfunId', 'UserId', 'createtime', 'oppen_udid', 
                       'registertime', 'result', 'reward', 'server_id', 
                       'success_rate', 'user_id', 'user_name'])
    
    
for i in loop_folder( os.getcwd()):
    temp_data = pd.read_json( 
            str( os.getcwd() + '\\' + i + '\\' + i + r"_tb_log_fishing.json"), 
            lines = True)
    print( str( i + r"_tb_log_fishing.json... Done!" ))
    result = pd.concat( [ result, temp_data])
    

writer = pd.ExcelWriter( r'D:\Work\201805\20180508 - 日志\fishing.xlsx')
data_all.to_excel( writer, index = False, encoding = 'utf-8', sheet_name = 'Sheet')
writer.save()