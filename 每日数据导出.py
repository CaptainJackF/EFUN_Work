# -*- coding: utf-8 -*-
"""
Created on Thu May  3 14:56:32 2018

@author: Efun
"""


import datetime
import MySQLdb as mdb
import pandas as pd

connection = mdb.connect( host = '210.59.244.161',
                          user = 'efun_select',
                          password = 'uPOX8bl+z%Q;EkA',
                          db = 'efun_global',
                          charset = 'utf8')

def date_calculate( n1 = 6, n2 = 1):
    '''
        n1: 用于计算开始时间
        n2: 用于计算结束时间, 即昨天的日期。
    '''
    today = datetime.date.today() 
    start_date = today - datetime.timedelta( days = n1)  
    end_date = today - datetime.timedelta( days = n2)  
    return( start_date.strftime( '%Y-%m-%d'), end_date.strftime( '%Y-%m-%d') )


sql_str = str(
    '''SELECT
        c.day_date, c.OsName,
        CASE WHEN c.lifetime <= 0 THEN '0天'
             WHEN c.lifetime = 1 THEN '1天'
                 WHEN c.lifetime > 1 AND c.lifetime <= 3 THEN '2-3天'
                 WHEN c.lifetime > 3 AND c.lifetime <= 7 THEN '4-7天'
                 WHEN c.lifetime > 7 AND c.lifetime <= 15 THEN '8-15天'
                 WHEN c.lifetime > 15 AND c.lifetime <= 30 THEN '16-30天'
                 WHEN c.lifetime > 30 AND c.lifetime <= 60 THEN '31-60天'
                 WHEN c.lifetime > 60 AND c.lifetime <= 90 THEN '61-90天'
                 WHEN c.lifetime > 91 AND c.lifetime <= 120 THEN '91-120天'
             ELSE '>120天'
        END lifetime_group,
        SUM(c.dau_total) AS dau_group
        FROM(
        SELECT 
            a.day_login AS day_date,
            OsName,
            COUNT(DISTINCT(a.EfunId)) AS dau_total,
            DATEDIFF(a.day_login, b.day_reg)+1 AS lifetime
        FROM(
                SELECT DATE(TIMESTAMP(CreateTime, '08:00:00')) AS day_login, EfunId
                FROM  tb_log_login_20180120 
                WHERE CreateTime BETWEEN '%s 16:00:00' AND '%s 15:59:59'
        )a
        INNER JOIN(
                SELECT DATE(TIMESTAMP(RegisterTime, '08:00:00')) AS day_reg, EfunId, OsName
                FROM tb_log_register
                WHERE EfunId > 300000000
        )b
        Where a.EfunId = b. EfunId
        GROUP BY day_date, lifetime, OsName
        )c
        GROUP BY lifetime_group,c.day_date, c.OsName
        order by osname
''' %( date_calculate()[0], date_calculate()[1])) ## 用于替换上文中的 %s 内容。


df = pd.read_sql( sql_str, con = connection)


writer = pd.ExcelWriter( r'C:\Users\Efun\Desktop\output.xlsx')
df.to_excel( writer, index = False, encoding = 'utf-8', sheet_name = 'Sheet')
writer.save()