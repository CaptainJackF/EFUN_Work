# -*- coding: utf-8 -*-
"""
Created on Mon May  7 21:54:59 2018

@author: Efun
"""

import pandas as pd
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
    
   
##行数
print( len( result)) ## 6896880

## 成功数
success_num = result[ result.result == 1] 
gp_success = success_num.groupby( by = 'UserId')  
gp_success_result = gp_success.size() 
gp_success_result.to_csv( 'success_result.csv')

## 失败数
fail_num = result[ result.result == 0] 
gp_fail = fail_num.groupby( by = 'UserId')  
gp_fail_result = gp_fail.size() 
gp_fail_result.to_csv( 'fail_result.csv')


## 鱼/宝箱数
item_num = result[ result.reward > 0] 
gp_item = item_num.groupby( by = 'reward')  
gp_item_result = gp_item.size() 
gp_item_result.to_csv( 'item_result.csv')


'''
writer = pd.ExcelWriter( r'D:\Work\201805\20180508 - 日志\fishing.xlsx')
data_all.to_excel( writer, index = False, encoding = 'utf-8', sheet_name = 'Sheet')
writer.save()
'''

## Json输出
'''
json_result = result.to_json( orient='split')  
fileObject = open( 'json_result.json', 'w')  
fileObject.write( json_result)  
fileObject.close() 
'''
