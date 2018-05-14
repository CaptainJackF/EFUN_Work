# -*- coding: utf-8 -*-
"""
Created on Mon May  7 21:54:59 2018

@author: Efun
"""

import pandas as pd
import os



os.chdir( r"D:\Python_Working_Directory\Date\Fishing\FishingLog")
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
    
'''
for i in loop_folder( os.getcwd()):
    temp_data = pd.read_json( 
            str( os.getcwd() + '\\' + i + '\\' + i + r"_tb_log_fishing.json"), 
            lines = True)
    print( str( i + r"_tb_log_fishing.json... Done!" ))
    result = pd.concat( [ result, temp_data])

## Json输出
json_result = result.to_json( orient='split')  
fileObject = open( 'json_result.json', 'w')  
fileObject.write( json_result)  
fileObject.close() 

'''

## result =  pd.concat( [ result, pd.read_json( "json_result.json", lines = True)]) 

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


## 读取充值玩家首充时间
## 测试玩家 2298180, 85服, 5.1充值
print( result[ result.server_id == 85, result.UserId == 2298180] )

fishing_recharge = pd.read_excel( r"D:\Python_Working_Directory\Date\Fishing\recharge.xlsx", 
                                 sheetname = "钓鱼礼包首充时间")

## Merge
result_pay = pd.merge( result, fishing_recharge, 
                      on = [ 'server_id', 'UserId'])

## csv output
result_pay.to_csv( r"C:\Users\Efun\Desktop\20180516 - E180503活动分析\result_pay.csv")
