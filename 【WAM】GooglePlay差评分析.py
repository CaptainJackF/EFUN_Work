# -*- coding: utf-8 -*-
"""
Created on Sun May  5 20:41:09 2019

@author: Efun
"""


# 文件操作
import numpy as np
import pandas as pd
import os

# 分词
import jieba
import nltk


def function1( sum_list = [1,2,3]):
    '''
        sum_list: 待求和的列表
    '''
    result_list = []
    for i in sum_list:
        result_list.append( i*i) 
        
    return( sum( result_list))
    
    
## 读取文件
Test_File = pd.read_excel( r'D:\Work\201905\20190504 - 文本分析\Test_File.xlsx')


def fenci( File = Test_File):
    for i in range( len( File)):
        Test_File['分词后'][i] = nltk.word_tokenize( File['英语'][i])
    return( Test_File)

Test_File = fenci()


def search_words( 
        Ori_text = Test_File['分词后'][2], 
        Words = ['donate', 'donater', 'PTW', 'P', 'pay 2 win', 'pay 2 win', 'money', 'gold', 'buy', 'pay', 'win', 'package', 'Optimized'],
        Tags = '氪金'
        ):
    '''
        Ori_text: 待求和的列表
        words:
        Tags:
    '''
    Temp_Class = []
    for i in Words:
        i in Ori_text
        Temp_Class.append( i in Ori_text)
    if sum( Temp_Class) >= 1:
        return( Temp_Class, sum( Temp_Class), Tags)
    else:
        return( Temp_Class, sum( Temp_Class), np.nan)


def Loop_File( File = Test_File):
    for i in range( len( File)):
        a,Test_File['关键词出现数量'][i],Test_File['标签'][i] = search_words( Ori_text = File['分词后'][i])
    return( Test_File)


Test_File = Loop_File()