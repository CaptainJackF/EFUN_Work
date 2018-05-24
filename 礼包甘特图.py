# -*- coding: utf-8 -*-
"""
Created on Tue May 22 21:43:44 2018

@author: Efun
"""

import pandas as pd
import os
import datetime

os.chdir( r'C:\Users\Efun\Desktop')

def date_calculate( n1 = 6, n2 = 1):
    '''
        n1: 用于计算开始时间
        n2: 用于计算结束时间, 即昨天的日期。
    '''
    today = datetime.date.today() 
    start_date = today - datetime.timedelta( days = n1)  
    end_date = today - datetime.timedelta( days = n2)  
    return( start_date.strftime( '%Y-%m-%d'), end_date.strftime( '%Y-%m-%d') )

print(  date_calculate()[0])

File = r"C:\Users\Efun\Desktop\Efun充值列表.csv"
pwd = os.getcwd()

efun = pd.read_csv( r"C:\Users\Efun\Desktop\Efun充值列表.csv")