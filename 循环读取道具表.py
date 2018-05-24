# -*- coding: utf-8 -*-
"""
Created on Thu May 24 10:29:39 2018

@author: Efun
"""

import MySQLdb as mdb
import pandas as pd
import numpy as np

connection = mdb.connect( host = '210.59.244.161',
                          user = 'efun_select',
                          password = 'uPOX8bl+z%Q;EkA',
                          db = 'efun_global',
                          charset = 'utf8')


result = pd.DataFrame( columns = 
                      ['server_id', 'user_id', 'itemname', 'number', 
                       'itemtype', 'func', 'isbypay'])
    

for i in range(110,120):
    sql_str = str(
        '''SELECT
            	server_id,
            	user_id,
            	itemname,
            	number,
            	itemtype,
            	func,
                isbypay
            FROM
            	`tb_log_item_201804`
            WHERE 
                CreateTime > '2018-05-22 23:59:59'
                AND itemId = 86001
                AND Type = 1
                AND server_id = %s
            ;
    ''' % i) ## 用于替换上文中的 %s 内容。
    
    #print( sql_str)

    temp_data = pd.read_sql( sql_str, con = connection)
    result = pd.concat( [ result, temp_data])
    

## 成功数
pay_num = result[ result.isbypay == 1] 
print( np.sum( pay_num.number))

nopay_num = result[ result.isbypay == 2] 
print( np.sum( nopay_num.number))


writer = pd.ExcelWriter( r'C:\Users\Efun\Desktop\86001.xlsx')
result.to_excel( writer, index = False, encoding = 'utf-8', sheet_name = 'Sheet')
writer.save()

## 理论最高
