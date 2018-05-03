# -*- coding: utf-8 -*-
"""
Created on Thu May  3 20:41:08 2018

@author: Efun
"""


import json
import pandas as pd
import requests
from pandas.io.json import json_normalize

data = pd.read_json( r'C:\Users\Efun\Desktop\reward.json', lines=True)

writer = pd.ExcelWriter( r'C:\Users\Efun\Desktop\json.xlsx')
data.to_excel( writer, index = False, encoding = 'utf-8', sheet_name = 'Sheet')
writer.save()