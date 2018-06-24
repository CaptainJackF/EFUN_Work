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
from math import isnan

Dwarven = pd.read_excel( r'C:\Users\CaptainJack\Desktop\新建文件夹\矮人宝藏.xlsx',
                        sheetname = '0614')
print( Dwarven.columns)

## 筛选列
Dwarven_Gem = Dwarven.loc[210:220, ['server_id', 'user_id', 'userId', 'UserName', 'level_city',
                              'Gem1', 'Gem2', 'Gem3', 'Gem4', 'Gem5', 'Gem6']] 

## 构造json格式
Dwarven_Gem['Test'] = Dwarven_Gem.apply( lambda i: i['server_id'] +1, axis = 1)

type( Dwarven_Gem.to_json( orient = 'records'))

json_normalize( np.array( Dwarven_Gem.to_json( orient = 'records')).tolist(), 'Gem1',
               ['server_id', 'user_id', 'userId', 'UserName', 'level_city',
                ['Gem2', 'Gem3', 'Gem4', 'Gem5', 'Gem6']])

json_d = json.loads( np.array( Dwarven_Gem.to_json( orient = 'records')).tolist())
data = Dwarven_Gem = Dwarven.loc[210:220, ['server_id', 'user_id', 'userId', 'UserName', 'level_city', 'Gem1']] 
key_col = ['server_id', 'user_id']
json_col = 'Gem1'

def one_json_record( data, key_col, json_col):
    '''
        data: 需要解析的dataframe;
        key_col:
        json_col:    
    '''
    
    data.loc[ :, json_col] = data.apply( 
            lambda i: np.nan if isnan( i[json_col]) else json.loads( i[json_col]), axis = 1
                                     )
    json_d = json.loads( np.array( data.to_json( orient = 'records')).tolist())
    
    json_normalize( json_d)





data = [{'Gem1': [{"1_item":"82081","1_level":"1","2_item":0,"2_level":0,"3_item":0,"3_level":0}],
  'Gem2': [{"1_item":"82071","1_level":"1","2_item":0,"2_level":0,"3_item":0,"3_level":0}],
  'Gem3': [{"1_item":"82101","1_level":"1","2_item":0,"2_level":0,"3_item":0,"3_level":0}],
  'Gem4': [{"1_item":"82091","1_level":"1","2_item":0,"2_level":0,"3_item":0,"3_level":0}],
  'Gem5': [{"1_item":"82071","1_level":"1","2_item":0,"2_level":0,"3_item":0,"3_level":0}],
  'Gem6': None,
  'Test': 4,
  'UserName': 'Кирск',
  'level_city': 22,
  'server_id': 3,
  'userId': 44816,
  'user_id': 20001},
    {'Gem1':[],
  'Gem2': [{"1_item":"82071","1_level":"1","2_item":0,"2_level":0,"3_item":0,"3_level":0}],
  'Gem3': [{"1_item":"82101","1_level":"1","2_item":0,"2_level":0,"3_item":0,"3_level":0}],
  'Gem4': [{"1_item":"82091","1_level":"1","2_item":0,"2_level":0,"3_item":0,"3_level":0}],
  'Gem5': [{"1_item":"82071","1_level":"1","2_item":0,"2_level":0,"3_item":0,"3_level":0}],
  'Gem6': None,
  'Test': 4,
  'UserName': 'Кирск1',
  'level_city': 22,
  'server_id': 3,
  'userId': 44817,
  'user_id': 20002}]

json_normalize( data, 'Gem1', ['userId','level_city'] )
