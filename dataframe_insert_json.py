# -*- coding: utf-8 -*-
"""
Created on Sat Jun 23 10:00:55 2018

@author: CaptainJack
"""

## 解析json
import pandas as pd
import numpy as np
from pandas.io.json import json_normalize
import json
import MySQLdb as mdb

'''
## 本地测试数据
Dwarven = pd.read_excel( r'C:\Users\\Efun\Desktop\新建文件夹\矮人宝藏.xlsx',
                        sheet_name = '0614')

## 筛选行列, 测试用
Dwarven_Gem = Dwarven.loc[210:220, ['server_id', 'user_id', 'userId', 'UserName', 'level_city',
                              'Gem1', 'Gem2', 'Gem3', 'Gem4', 'Gem5', 'Gem6']] 

data = Dwarven_Gem = Dwarven.loc[210:220, ['server_id', 'user_id', 'userId', 'UserName', 'level_city', 'Gem1']] 
key_col = ['server_id', 'user_id']
json_col = 'Gem1'
'''


connection = mdb.connect( host = '210.59.244.161',
                          user = 'efun_select',
                          password = 'uPOX8bl+z%Q;EkA',
                          db = 'efun_global',
                          charset = 'utf8')

sql_str = str(
    '''
    SELECT
        server_id, user_id, userId, user_name, level_city,
        gem1, gem2, gem3, gem4, gem5, gem6
    FROM
        `tb_log_lordequipinfo_day`
    WHERE 
        CreateTime between '2018-06-13 00:00:00' AND '2018-06-13 23:59:59'
    ;
''') 
    

Dwarven_Gem = pd.read_sql( sql_str, con = connection)


def one_json_record( data, key_col, json_col):
    '''
        data: 需要解析的dataframe;
        key_col:
        json_col: 
    '''
    data = data.fillna( 0)      ## 缺失值填充, 方便下面做判断及替换
    '''
    data.loc[ :, json_col] = data.apply( 
            lambda i: list() if i[json_col] == 0 else [ json.loads( i[json_col])], axis = 1)  
    '''
    data.loc[ :, json_col] = data.apply( 
            lambda i: list() if i[json_col] == '' else [ json.loads( i[json_col])], axis = 1)  
    ##  apply 结合 Lambda是遍历 Dataframe的方法之一
    ##  设法将 str格式的'json'数据转化为 [json] 形式
    
    json_d = json.loads( np.array( data.to_json( orient = 'records')).tolist())  ## 将整个Dataframe构造成一个json格式的数据
    
    data = json_normalize( json_d, json_col, key_col)  ##　解析Json, 这里如果解析的变量为空list，则不会输出结果，可以起到过滤作用
    data['Position'] = json_col     ##

    return( data)
    del [json_d, data]  ## 删除变量
    

Gem_Nomale = pd.DataFrame( columns = 
                          ['Position', '1_item', '1_level', '2_item', '2_level', '3_item', '3_level', 
                           'server_id', 'user_id', 'userId', 'user_name', 'level_city'])

for i in Dwarven_Gem.columns[ -6::]:
    
    data = one_json_record( data = Dwarven_Gem, 
                           key_col = ['server_id', 'user_id', 'userId', 'user_name', 'level_city'], 
                           json_col = i)
    print(i)
    Gem_Nomale = pd.concat( [ Gem_Nomale, data])
    
    
writer = pd.ExcelWriter( r'C:\Users\Efun\Desktop\0607活动分析\Gem.xlsx')
Gem_Nomale.to_excel( writer, index = False, encoding = 'utf-8', sheet_name = 'Sheet')
writer.save()
