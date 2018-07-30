# -*- coding: utf-8 -*-
"""
Created on Thu Jun 28 17:39:44 2018

@author: Efun
"""

## 报表自动化

import pandas as pd
import MySQLdb as mdb
import datetime
import time
import copy

connection = mdb.connect( host = '210.59.244.161',
                          user = 'efun_select',
                          password = 'uPOX8bl+z%Q;EkA',
                          db = 'efun_global',
                          charset = 'utf8')


def date_calculate( n1 = 21, n2 = 1):
    '''
        n1: 用于计算开始时间
        n2: 用于计算结束时间, 即昨天的日期。
    '''
    today = datetime.date.today() 
    start_date = today - datetime.timedelta( days = n1)  
    end_date = today - datetime.timedelta( days = n2)  
    return( start_date.strftime( '%Y-%m-%d'), end_date.strftime( '%Y-%m-%d') )
    
def weeknum( date = datetime.date.today().strftime( '%Y-%m-%d') ):
    '''
        字符串日期转周几
    ''' 
    return( time.strptime( date,"%Y-%m-%d").tm_wday + 1)
    
    
    
    
## ------ Actual Data ------
## And Actual Data
And_actual_str = str(
    '''
    SELECT
    	Date,
       "" as Week,
    	ROUND( SUM( paySum),2) as Recharge,# -*- coding: utf-8 -*-# -*- coding: utf-8 -*-
"""
Created on Thu Jun 28 17:39:44 2018

@author: Efun
"""

## 报表自动化

import pandas as pd
import numpy as np
import MySQLdb as mdb
import datetime
import time
import copy

connection = mdb.connect( host = '210.59.244.161',
                          user = 'efun_select',
                          password = 'uPOX8bl+z%Q;EkA',
                          db = 'efun_global',
                          charset = 'utf8')


def date_calculate( n1 = 21, n2 = 1):
    '''
        n1: 用于计算开始时间
        n2: 用于计算结束时间, 即昨天的日期。
    '''
    today = datetime.date.today() 
    start_date = today - datetime.timedelta( days = n1)  
    end_date = today - datetime.timedelta( days = n2)  
    return( start_date.strftime( '%Y-%m-%d'), end_date.strftime( '%Y-%m-%d') )
    
def weeknum( date = datetime.date.today().strftime( '%Y-%m-%d') ):
    '''
        字符串日期转周几
    ''' 
    return( time.strptime( date,"%Y-%m-%d").tm_wday + 1)
    
    
    
    
## ------ Actual Data ------
## And Actual Data
And_actual_str = str(
    '''
    SELECT
    	Date,
       "" as Week,
    	ROUND( SUM( paySum),2) as Recharge,
       "" as Cost, 
       SUM( registerCnt) as DNU,
       "" as CPU, 
      	SUM( loginCnt) as DAU,
       "" as EDAU, 
    	SUM( payCnt) as Pay_Num 
    FROM
    	`t_data_track`
    WHERE
    	Date BETWEEN '%s' AND '%s'
    AND advertiser != 'untrusted devices'   ## 去除假量
    AND platform = 'android'
    AND gameCode = 'stgl'
    GROUP BY Date
    ORDER BY Date;
''' %( date_calculate()[0], date_calculate()[1])) 
    

And_actual_data = pd.read_sql( And_actual_str, con = connection)
And_actual_data.loc[ :, "Week"] = And_actual_data.apply( 
            lambda i: weeknum( date = str( i[ "Date"])), axis = 1)

And_actual_data[ "Week"] = And_actual_data.apply( lambda i: weeknum( str( i['Date'])) , axis = 1)

## iOS Actual Data
iOS_actual_str = str(
    '''
    SELECT
    	Date,
       "" as Week,
    	ROUND( SUM( paySum),2) as Recharge,
       "" as Cost, 
       SUM( registerCnt) as DNU,
       "" as CPU, 
      	SUM( loginCnt) as DAU,
       "" as EDAU, 
    	SUM( payCnt) as Pay_Num 
    FROM
    	`t_data_track`
    WHERE
    	Date BETWEEN '%s' AND '%s'
    AND advertiser != 'untrusted devices'   ## 去除假量
    AND platform = 'ios'
    AND gameCode = 'stgl'
    GROUP BY Date
    ORDER BY Date;
''' %( date_calculate()[0], date_calculate()[1])) 
    

iOS_actual_data = pd.read_sql( iOS_actual_str, con = connection)
iOS_actual_data.loc[ :, "Week"] = iOS_actual_data.apply( 
            lambda i: weeknum( date = str( i[ "Date"])), axis = 1)


## ------ DNU 占比 ------
Index = pd.DataFrame({
        'Index1': ["T1", "ROW", "AU", "CA", "US", "GB", "DE", "FR", "RU", "TW", "HK", "JP", "KR"],
        'Index': ["01.T1", "02.ROW", "03.AU", "04.CA", "05.US", "06.GB", "07.DE", "08.FR", "09.RU", "10.TW", "11.HK", "12.JP", "13.KR"]
        })
## "CH", "AT", "CN", "MO" 剔除, 方面下面做 Merge时直接剔除这四个地区
    
And_Country_Dnu_str = str(
    '''
        SELECT
        	Date,
        	registerAreaCode as Index1,
        	SUM( IFNULL( registerCnt,0)) AS DNU
        FROM
        	`t_data_track`
        WHERE
        	Date BETWEEN '%s' AND '%s'
        AND advertiser != 'untrusted devices' ## 去除假量
        AND platform = 'android'
        AND gameCode = 'stgl'
        AND tier = 'T1'
        GROUP BY
        	Date, Index1, tier
        ORDER BY
        	Date;
''' %( date_calculate()[0], date_calculate()[1])) 
    
And_Tier_Dnu_str = str(
    '''
        SELECT
        	Date,
        	CASE 
        		WHEN tier = 'T1' THEN 'T1'
        		WHEN tier <> 'T1' THEN 'ROW'
        	END AS Index1,
        	SUM( IFNULL( registerCnt,0)) AS DNU
        FROM
        	`t_data_track`
        WHERE
        	Date BETWEEN '%s' AND '%s'
        AND advertiser != 'untrusted devices' ## 去除假量
        AND platform = 'android'
        AND gameCode = 'stgl'
        GROUP BY
        	Date, Index1
        ORDER BY
        	Date;
''' %( date_calculate()[0], date_calculate()[1])) 
    

And_Country_Dnu_data = pd.read_sql( And_Country_Dnu_str, con = connection)
And_Tier_Dnu_str_data = pd.read_sql( And_Tier_Dnu_str, con = connection)
And_Country_Dnu_data = pd.concat( [ And_Country_Dnu_data, And_Tier_Dnu_str_data])
And_Country_Dnu_data = pd.merge( And_Country_Dnu_data, Index, on = 'Index1')

And = pd.pivot_table( And_Country_Dnu_data, index = ['Date'], columns=['Index'], values = ['DNU'])
And = And.fillna(0)
And['DNU', '00.DNU'] = And.apply( lambda i: i['DNU', "01.T1"] + i['DNU', "02.ROW"] , axis = 1)
And = And.sort_index( axis = 1)
# And.ix[ :, And.columns.get_level_values(1).isin({"00.DNU", "01.T1", "02.ROW", "03.AU", "04.CA", "05.US", "06.GB", "07.DE", "08.FR", "09.RU", "10.TW", "11.HK", "12.JP", "13.KR"})]

And_Market = copy.deepcopy( And)
And_Market['DNU', '1.T1-EN'] = And_Market.apply( lambda i:  i['DNU', "03.AU"] + i['DNU', "04.CA"] + i['DNU', "05.US"] + i['DNU', "06.GB"] , axis = 1)
And_Market['DNU', '2.DE'] = And_Market.apply( lambda i:  i['DNU', "07.DE"] , axis = 1)
And_Market['DNU', '3.RU'] = And_Market.apply( lambda i:  i['DNU', "09.RU"] , axis = 1)
And_Market['DNU', '4.HK/TW/JP/KR'] = And_Market.apply( lambda i:  i['DNU', "10.TW"] + i['DNU', "11.HK"] + i['DNU', "12.JP"] + i['DNU', "13.KR"] , axis = 1)
## 市场对于 ROW的定义与后台不同，需要用总DNU减去上述4个组合
And_Market['DNU', '5.ROW'] = And_Market.apply( 
        lambda i: i['DNU', "00.DNU"] - i['DNU', '1.T1-EN'] - i['DNU', '2.DE'] - i['DNU', '3.RU'] - i['DNU', '4.HK/TW/JP/KR'] , axis = 1)
##And_Market['DNU', '5.ROW'] = And_Market.apply( lambda i:  i['DNU', "02.ROW"] , axis = 1)

And_Market = And_Market.ix[ :, And_Market.columns.get_level_values(1).isin({"1.T1-EN", "2.DE", "3.RU", "4.HK/TW/JP/KR", "5.ROW"})]


iOS_Country_Dnu_str = str(
    '''
        SELECT
        	Date,
        	registerAreaCode as Index1,
        	SUM( IFNULL( registerCnt,0)) AS DNU
        FROM
        	`t_data_track`
        WHERE
        	Date BETWEEN '%s' AND '%s'
        AND advertiser != 'untrusted devices' ## 去除假量
        AND platform = 'ios'
        AND gameCode = 'stgl'
        AND tier = 'T1'
        GROUP BY
        	Date, Index1, tier
        ORDER BY
        	Date;
''' %( date_calculate()[0], date_calculate()[1])) 
    
iOS_Tier_Dnu_str = str(
    '''
        SELECT
        	Date,
        	CASE 
        		WHEN tier = 'T1' THEN 'T1'
        		WHEN tier <> 'T1' THEN 'ROW'
        	END AS Index1,
        	SUM( IFNULL( registerCnt,0)) AS DNU
        FROM
        	`t_data_track`
        WHERE
        	Date BETWEEN '%s' AND '%s'
        AND advertiser != 'untrusted devices' ## 去除假量
        AND platform = 'iOS'
        AND gameCode = 'stgl'
        GROUP BY
        	Date, Index1
        ORDER BY
        	Date;
''' %( date_calculate()[0], date_calculate()[1])) 
    

iOS_Country_Dnu_data = pd.read_sql( iOS_Country_Dnu_str, con = connection)
iOS_Tier_Dnu_str_data = pd.read_sql( iOS_Tier_Dnu_str, con = connection)
iOS_Country_Dnu_data = pd.concat( [ iOS_Country_Dnu_data, iOS_Tier_Dnu_str_data])
iOS_Country_Dnu_data = pd.merge( iOS_Country_Dnu_data, Index, on = 'Index1')

iOS = pd.pivot_table( iOS_Country_Dnu_data, index = ['Date'], columns=['Index'], values = ['DNU'])
iOS = iOS.fillna(0)
iOS['DNU', '00.DNU'] = iOS.apply( lambda i: i['DNU', "01.T1"] + i['DNU', "02.ROW"] , axis = 1)
iOS = iOS.sort_index( axis = 1)
#iOS.ix[ :, iOS.columns.get_level_values(1).isin({"01.T1", "02.ROW", "03.AU", "04.CA", "05.US", "06.GB", "07.DE", "08.FR", "09.RU", "10.TW", "11.HK", "12.JP", "13.KR"})]

iOS_Market = copy.deepcopy( iOS)
iOS_Market['DNU', '1.T1-EN'] = iOS_Market.apply( lambda i:  i['DNU', "03.AU"] + i['DNU', "04.CA"] + i['DNU', "05.US"] + i['DNU', "06.GB"] , axis = 1)
iOS_Market['DNU', '2.DE'] = iOS_Market.apply( lambda i:  i['DNU', "07.DE"] , axis = 1)
iOS_Market['DNU', '3.RU'] = iOS_Market.apply( lambda i:  i['DNU', "09.RU"] , axis = 1)
iOS_Market['DNU', '4.HK/TW/JP/KR'] = iOS_Market.apply( lambda i:  i['DNU', "10.TW"] + i['DNU', "11.HK"] + i['DNU', "12.JP"] + i['DNU', "13.KR"] , axis = 1)
## 市场对于 ROW的定义与后台不同，需要用总DNU减去上述4个组合
iOS_Market['DNU', '5.ROW'] = iOS_Market.apply( 
        lambda i: i['DNU', "00.DNU"] - i['DNU', '1.T1-EN'] - i['DNU', '2.DE'] - i['DNU', '3.RU'] - i['DNU', '4.HK/TW/JP/KR'] , axis = 1)
##iOS_Market['DNU', '5.ROW'] = iOS_Market.apply( lambda i:  i['DNU', "02.ROW"] , axis = 1)

iOS_Market = iOS_Market.ix[ :, iOS_Market.columns.get_level_values(1).isin({"1.T1-EN", "2.DE", "3.RU", "4.HK/TW/JP/KR", "5.ROW"})]


## ------ 留存Cohort ------
And_Cohort = str(
    '''
        SELECT 
            a.Date, a.registerDate, a.xday, 
            sum( a.loginCnt1) as login, sum( a.registerCnt1) as reg, 
            round( sum( a.paySum1)*0.49,2) as Recharge 
        FROM
        (
        	SELECT
        		Date, registerDate, DATEDIFF( Date, registerDate) +1 as xday, advertiser, countryGroup, 
        		IFNULL( loginCnt,0) as loginCnt1, IFNULL( registerCnt,0) as registerCnt1, IFNULL( paySum,0) as paySum1
        	FROM
        		`t_data_track`
        	WHERE
        		registerDate BETWEEN '%s' AND '%s'
        	AND Date BETWEEN '%s' AND '%s'
        	AND advertiser != 'untrusted devices'  ## 假量
          AND platform = 'android'
        	AND gameCode = 'stgl'
        ) a
        GROUP BY a.Date, a.registerDate, a.xday
        HAVING a.xday <= 7;
''' %( date_calculate()[0], date_calculate()[1], date_calculate()[0], date_calculate()[1])) 

And_Cohort_Data = pd.read_sql( And_Cohort, con = connection)
And_Cohort_Data = pd.pivot_table( And_Cohort_Data, index = ['registerDate'], columns=['xday'], values = ['reg','login', 'Recharge'])
#And_Cohort_Data = And_Cohort_Data.fillna(0)

And_Cohort_Data['留存', 'DNU'] = And_Cohort_Data['reg', 1]
And_Cohort_Data['留存', '次留'] = And_Cohort_Data.apply( lambda i: i['login', 2]/i['reg', 1] , axis = 1)
And_Cohort_Data['留存', '7留'] = And_Cohort_Data.apply( lambda i: i['login', 7]/i['reg', 1] , axis = 1)
And_Cohort_Data['留存', 'CPU'] = ''

And_Cohort_Data['LTV', '1 LTV'] = And_Cohort_Data.apply( 
        lambda i: i['Recharge', 1]/i['reg', 1] , axis = 1)
And_Cohort_Data['LTV', '1 ROI'] = ''
And_Cohort_Data['LTV', '3 LTV'] = And_Cohort_Data.apply( 
        lambda i: '-' if np.isnan( i['Recharge', 3]) else (i['Recharge', 1]+i['Recharge', 2]+i['Recharge', 3])/i['reg', 1] , axis = 1)
And_Cohort_Data['LTV', '3 ROI'] = ''
And_Cohort_Data['LTV', '7 LTV'] = And_Cohort_Data.apply( 
        lambda i: '-' if np.isnan( i['Recharge', 7]) else (i['Recharge', 1]+i['Recharge', 2]+i['Recharge', 3]+i['Recharge', 4]+i['Recharge', 5]+i['Recharge', 6]+i['Recharge', 7])/i['reg', 1] , axis = 1)


And_Cohort_Data_Op = And_Cohort_Data.ix[ :, And_Cohort_Data.columns.get_level_values(1).isin({"DNU", "次留", "7留", "CPU", "3 LTV", "3 ROI", "7 LTV"})]
## 市场日报
And_Cohort_Data_Market = And_Cohort_Data.ix[ :, And_Cohort_Data.columns.get_level_values(1).isin({"DNU", "次留", "1 LTV", "1 ROI", "3 LTV", "3 ROI", "7 LTV"})]  


iOS_Cohort = str(
    '''
        SELECT 
            a.Date, a.registerDate, a.xday, 
            sum( a.loginCnt1) as login, sum( a.registerCnt1) as reg, 
            round( sum( a.paySum1)*0.49,2) as Recharge 
        FROM
        (
        	SELECT
        		Date, registerDate, DATEDIFF( Date, registerDate) +1 as xday, advertiser, countryGroup, 
        		IFNULL( loginCnt,0) as loginCnt1, IFNULL( registerCnt,0) as registerCnt1, IFNULL( paySum,0) as paySum1
        	FROM
        		`t_data_track`
        	WHERE
        		registerDate BETWEEN '%s' AND '%s'
        	AND Date BETWEEN '%s' AND '%s'
        	AND advertiser != 'untrusted devices'  ## 假量
          AND platform = 'ios'
        	AND gameCode = 'stgl'
        ) a
        GROUP BY a.Date, a.registerDate, a.xday
        HAVING a.xday <= 7;
''' %( date_calculate()[0], date_calculate()[1], date_calculate()[0], date_calculate()[1])) 

iOS_Cohort_Data = pd.read_sql( iOS_Cohort, con = connection)
iOS_Cohort_Data = pd.pivot_table( iOS_Cohort_Data, index = ['registerDate'], columns=['xday'], values = ['reg','login', 'Recharge'])
#iOS_Cohort_Data = iOS_Cohort_Data.fillna(0)

iOS_Cohort_Data['留存', 'DNU'] = iOS_Cohort_Data['reg', 1]
iOS_Cohort_Data['留存', '次留'] = iOS_Cohort_Data.apply( lambda i: i['login', 2]/i['reg', 1] , axis = 1)
iOS_Cohort_Data['留存', '7留'] = iOS_Cohort_Data.apply( lambda i: i['login', 7]/i['reg', 1] , axis = 1)
iOS_Cohort_Data['留存', 'CPU'] = ''

iOS_Cohort_Data['LTV', '1 LTV'] = iOS_Cohort_Data.apply( 
        lambda i: i['Recharge', 1]/i['reg', 1] , axis = 1)
iOS_Cohort_Data['LTV', '1 ROI'] = ''
iOS_Cohort_Data['LTV', '3 LTV'] = iOS_Cohort_Data.apply( 
        lambda i: '-' if np.isnan( i['Recharge', 3]) else (i['Recharge', 1]+i['Recharge', 2]+i['Recharge', 3])/i['reg', 1] , axis = 1)
iOS_Cohort_Data['LTV', '3 ROI'] = ''
iOS_Cohort_Data['LTV', '7 LTV'] = iOS_Cohort_Data.apply( 
        lambda i: '-' if np.isnan( i['Recharge', 7]) else (i['Recharge', 1]+i['Recharge', 2]+i['Recharge', 3]+i['Recharge', 4]+i['Recharge', 5]+i['Recharge', 6]+i['Recharge', 7])/i['reg', 1] , axis = 1)

iOS_Cohort_Data_Op = iOS_Cohort_Data.ix[ :, iOS_Cohort_Data.columns.get_level_values(1).isin({"DNU", "次留", "7留", "CPU", "3 LTV", "3 ROI", "7 LTV"})]
## 市场日报
iOS_Cohort_Data_Market = iOS_Cohort_Data.ix[ :, iOS_Cohort_Data.columns.get_level_values(1).isin({"DNU", "次留", "1 LTV", "1 ROI", "3 LTV", "3 ROI", "7 LTV"})]  


Whole_Cohor = str(
    '''
        SELECT 
            a.Date, "整体" as Type, a.registerDate, a.xday, 
            sum( a.loginCnt1) as login, sum( a.registerCnt1) as reg, round( sum( a.paySum1)*0.49,2) as Recharge 
        FROM
        (
        	SELECT
        		Date, registerDate, DATEDIFF( Date, registerDate) +1 as xday, advertiser, countryGroup, 
        		IFNULL( loginCnt,0) as loginCnt1, IFNULL( registerCnt,0) as registerCnt1, IFNULL( paySum,0) as paySum1
        	FROM
        		`t_data_track`
        	WHERE
        		registerDate BETWEEN '%s' AND '%s'
        	AND Date BETWEEN '%s' AND '%s'
        	AND advertiser != 'untrusted devices'  ## 假量
        	AND gameCode = 'stgl'
        ) a
        WHERE a.xday <= 7
        GROUP BY a.Date, a.registerDate, a.xday
        UNION
        SELECT 
            a.Date, "自然量" as Type, a.registerDate, a.xday, 
            sum( a.loginCnt1) as login, sum( a.registerCnt1) as reg, round( sum( a.paySum1)*0.49,2) as Recharge 
        FROM
        (
        	SELECT
        		Date, registerDate, DATEDIFF( Date, registerDate) +1 as xday, advertiser, countryGroup, 
        		IFNULL( loginCnt,0) as loginCnt1, IFNULL( registerCnt,0) as registerCnt1, IFNULL( paySum,0) as paySum1
        	FROM
        		`t_data_track`
        	WHERE
        		registerDate BETWEEN '%s' AND '%s'
        	AND Date BETWEEN '%s' AND '%s'
        	AND advertiser != 'untrusted devices'  ## 假量
        	AND advertiser = 'organic'
        	AND gameCode = 'stgl'
        ) a
        WHERE a.xday <= 7
        GROUP BY a.Date, a.registerDate, a.xday
        UNION
        SELECT 
            a.Date, "渠道量" as Type, a.registerDate, a.xday, 
            sum( a.loginCnt1) as login, sum( a.registerCnt1) as reg, round( sum( a.paySum1)*0.49,2) as Recharge  
        FROM
        (
        	SELECT
        		Date, registerDate, DATEDIFF( Date, registerDate) +1 as xday, advertiser, countryGroup, 
        		IFNULL( loginCnt,0) as loginCnt1, IFNULL( registerCnt,0) as registerCnt1, IFNULL( paySum,0) as paySum1
        	FROM
        		`t_data_track`
        	WHERE
        		registerDate BETWEEN '%s' AND '%s'
        	AND Date BETWEEN '%s' AND '%s'
        	AND advertiser != 'untrusted devices'  ## 假量
        	AND advertiser != 'organic'
        	AND gameCode = 'stgl'
        ) a
        WHERE a.xday <= 7
        GROUP BY a.Date, a.registerDate, a.xday;
''' %( date_calculate()[0], date_calculate()[1], date_calculate()[0], date_calculate()[1],
        date_calculate()[0], date_calculate()[1], date_calculate()[0], date_calculate()[1],
        date_calculate()[0], date_calculate()[1], date_calculate()[0], date_calculate()[1])) 

Whole_Cohoe_Data = pd.read_sql( Whole_Cohor, con = connection)
Whole_Cohoe_Data = pd.pivot_table( Whole_Cohoe_Data, index = ['registerDate'], columns=['Type','xday'], values = ['reg','login', 'Recharge'])
# Whole_Cohoe_Data = Whole_Cohoe_Data.fillna(0)

## ------ 整体留存 ------
Whole_Cohoe_Data['留存', '整体', 'DNU'] = Whole_Cohoe_Data['reg', '整体', 1]
Whole_Cohoe_Data['次日留存率', '整体', '次留1'] = Whole_Cohoe_Data.apply( lambda i: i['login', '整体', 2]/i['reg', '整体', 1] , axis = 1)
Whole_Cohoe_Data['次日留存率', '自然量', '次留2'] = Whole_Cohoe_Data.apply( lambda i: i['login', '自然量', 2]/i['reg', '自然量', 1] , axis = 1)
Whole_Cohoe_Data['次日留存率', '渠道量', '次留3'] = Whole_Cohoe_Data.apply( lambda i: i['login', '渠道量', 2]/i['reg', '渠道量', 1] , axis = 1)
Whole_Cohoe_Data['次日留存率', '整体', '7留1'] = Whole_Cohoe_Data.apply( lambda i: i['login', '整体', 7]/i['reg', '整体', 1] , axis = 1)
Whole_Cohoe_Data['次日留存率', '自然量', '7留2'] = Whole_Cohoe_Data.apply( lambda i: i['login', '自然量', 7]/i['reg', '自然量', 1] , axis = 1)
Whole_Cohoe_Data['次日留存率', '渠道量', '7留3'] = Whole_Cohoe_Data.apply( lambda i: i['login', '渠道量', 7]/i['reg', '渠道量', 1] , axis = 1)

Whole_Cohoe_Data1 = Whole_Cohoe_Data.ix[ :, Whole_Cohoe_Data.columns.get_level_values(2).isin({"DNU", "次留1", "次留2", "次留3", "7留1", "7留2", "7留3"})]


## ------ 整体LTV ------
Whole_Cohoe_Data['LTV', '整体', '1 ROI'] = ''
Whole_Cohoe_Data['LTV', '整体', '3 ROI'] = ''
Whole_Cohoe_Data['LTV', '整体', '7 ROI'] = ''
Whole_Cohoe_Data['LTV', '整体', '7ROI/3ROI'] = ''

Whole_Cohoe_Data['LTV', '整体', '1 LTV'] = Whole_Cohoe_Data.apply( 
        lambda i: (i['Recharge', '整体', 1])/i['reg', '整体', 1] , axis = 1)
Whole_Cohoe_Data['3LTV', '整体', '3 LTV1'] = Whole_Cohoe_Data.apply( 
        lambda i: '-' if np.isnan( i['Recharge', '整体', 3]) else (i['Recharge', '整体', 1]+i['Recharge', '整体', 2]+i['Recharge', '整体', 3])/i['reg', '整体', 1] , axis = 1)
Whole_Cohoe_Data['3LTV', '自然量', '3 LTV2'] = Whole_Cohoe_Data.apply( 
        lambda i: '-' if np.isnan( i['Recharge', '自然量', 3]) else (i['Recharge', '自然量', 1]+i['Recharge', '自然量', 2]+i['Recharge', '自然量', 3])/i['reg', '自然量', 1] , axis = 1)
Whole_Cohoe_Data['3LTV', '渠道量', '3 LTV3'] = Whole_Cohoe_Data.apply( 
        lambda i: '-' if np.isnan( i['Recharge', '渠道量', 3]) else (i['Recharge', '渠道量', 1]+i['Recharge', '渠道量', 2]+i['Recharge', '渠道量', 3])/i['reg', '渠道量', 1] , axis = 1)

Whole_Cohoe_Data['7LTV', '整体', '7 LTV1'] = Whole_Cohoe_Data.apply( 
        lambda i: '-' if np.isnan( i['Recharge', '整体', 7]) else (i['Recharge', '整体', 1]+i['Recharge', '整体', 2]+i['Recharge', '整体', 3]+i['Recharge', '整体', 4]+i['Recharge', '整体', 5]+i['Recharge', '整体', 6]+i['Recharge', '整体', 7])/i['reg', '整体', 1] , axis = 1)
Whole_Cohoe_Data['7LTV', '自然量', '7 LTV2'] = Whole_Cohoe_Data.apply( 
        lambda i: '-' if np.isnan( i['Recharge', '自然量', 7]) else (i['Recharge', '自然量', 1]+i['Recharge', '自然量', 2]+i['Recharge', '自然量', 3]+i['Recharge', '自然量', 4]+i['Recharge', '自然量', 5]+i['Recharge', '自然量', 6]+i['Recharge', '自然量', 7])/i['reg', '自然量', 1] , axis = 1)
Whole_Cohoe_Data['7LTV', '渠道量', '7 LTV3'] = Whole_Cohoe_Data.apply( 
        lambda i: '-' if np.isnan( i['Recharge', '渠道量', 7]) else (i['Recharge', '渠道量', 1]+i['Recharge', '渠道量', 2]+i['Recharge', '渠道量', 3]+i['Recharge', '渠道量', 4]+i['Recharge', '渠道量', 5]+i['Recharge', '渠道量', 6]+i['Recharge', '渠道量', 7])/i['reg', '渠道量', 1] , axis = 1)

Whole_Cohoe_Data2 = Whole_Cohoe_Data.ix[ :, Whole_Cohoe_Data.columns.get_level_values(2).isin({"DNU", "1 ROI", "3 ROI", "7 ROI", "7ROI/3ROI", "1 LTV", "3 LTV1", "3 LTV2", "3 LTV3", "7 LTV1", "7 LTV2", "7 LTV3"})]


## ------ DNU分生命周期 ------
## 登录表查询太慢了, 暂时不放在这个脚本中；


## ------ Marketing ------
def make_config():
    Week_config = copy.deepcopy( And_actual_data.loc[ :,"Date": "Week"])
    Week_config['Temp'] = Week_config.apply( 
            lambda i: '3天' if i['Week'] >= 3 and i['Week'] <= 5 else '4天', axis = 1)
    
    Week_config['Temp1'] = 1 
    for i in range( 1, len( Week_config)):
        if Week_config.loc[ i, 'Temp'] == Week_config.loc[ i-1, 'Temp']:
            Week_config.loc[ i, 'Temp1'] = Week_config.loc[ i-1, 'Temp1'] 
        else:
            Week_config.loc[ i, 'Temp1'] = Week_config.loc[ i-1, 'Temp1'] + 1
            
    Week_config1 = pd.DataFrame({ 'Temp1': list( range( min( Week_config['Temp1']), max( Week_config['Temp1']+1) ) ),
                                'Class' : '',
                                'Day_num' : ''})
    
    for i in Week_config1['Temp1']:
        Week_config1.loc[ i-1, 'Class'] = str( 
                str( i) + ". " +
                min( Week_config[ Week_config['Temp1'] == i]['Date']).strftime( '%m.%d') + 
                "-" + 
                max( Week_config[ Week_config['Temp1'] == i]['Date']).strftime( '%m.%d')
                )
        Week_config1.loc[ i-1, 'Day_num'] = len( Week_config[ Week_config['Temp1'] == i])
        
    Week_config = pd.merge( Week_config, Week_config1, on = 'Temp1')
    del Week_config1
    return( Week_config.loc[ :,['Date', 'Temp', 'Class', 'Day_num']])

Week_config = make_config()


## Android 市场周数据
And_Cohort_Counrty = str(
    '''
        SELECT 
            a.Date, a.registerDate, a.xday, registerAreaCode,
            sum( a.loginCnt1) as login, sum( a.registerCnt1) as reg, 
            round( sum( a.paySum1)*0.49,2) as Recharge 
        FROM
        (
        	SELECT
        		Date, registerDate, DATEDIFF( Date, registerDate) +1 as xday, advertiser, countryGroup, registerAreaCode,
        		IFNULL( loginCnt,0) as loginCnt1, IFNULL( registerCnt,0) as registerCnt1, IFNULL( paySum,0) as paySum1
        	FROM
        		`t_data_track`
        	WHERE
        		registerDate BETWEEN '%s' AND '%s'
        	AND Date BETWEEN '%s' AND '%s'
        	AND advertiser != 'untrusted devices'  ## 假量
          AND platform = 'android'
        	AND gameCode = 'stgl'
          AND tier = 'T1'
        ) a
        GROUP BY a.Date, a.registerDate, a.xday, registerAreaCode
        HAVING a.xday <= 7;
''' %( date_calculate()[0], date_calculate()[1], date_calculate()[0], date_calculate()[1])) 

And_Cohort_Counrty_Data = pd.read_sql( And_Cohort_Counrty, con = connection)
And_Cohort_Counrty_Data = pd.merge( And_Cohort_Counrty_Data, Week_config, left_on = 'registerDate', right_on = 'Date')

And_Cohort_Counrty_Data = pd.pivot_table( And_Cohort_Counrty_Data, index = ['Class'], columns=['registerAreaCode', 'xday'], values = ['reg','login', 'Recharge'])

# And_Cohort_Counrty_Data = And_Cohort_Counrty_Data.fillna(0)


## T1 -EN 
And_Cohort_Counrty_Data['Android数据', '1. T1-EN', '3 LTV'] = And_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','AU',1]+ i['Recharge','CA',1]+ i['Recharge','US',1]+ i['Recharge','GB',1]+
                    i['Recharge','AU',2]+ i['Recharge','CA',2]+ i['Recharge','US',2]+ i['Recharge','GB',2]+
                    i['Recharge','AU',3]+ i['Recharge','CA',3]+ i['Recharge','US',3]+ i['Recharge','GB',3])/( i['reg','AU',1]+ i['reg','CA',1]+ i['reg','US',1]+ i['reg','GB',1]) , axis = 1)

And_Cohort_Counrty_Data['Android数据', '1. T1-EN', '7 LTV'] = And_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','AU',1]+ i['Recharge','CA',1]+ i['Recharge','US',1]+ i['Recharge','GB',1]+
                    i['Recharge','AU',2]+ i['Recharge','CA',2]+ i['Recharge','US',2]+ i['Recharge','GB',2]+
                    i['Recharge','AU',3]+ i['Recharge','CA',3]+ i['Recharge','US',3]+ i['Recharge','GB',3]+
                    i['Recharge','AU',4]+ i['Recharge','CA',4]+ i['Recharge','US',4]+ i['Recharge','GB',4]+
                    i['Recharge','AU',5]+ i['Recharge','CA',5]+ i['Recharge','US',5]+ i['Recharge','GB',5]+
                    i['Recharge','AU',6]+ i['Recharge','CA',6]+ i['Recharge','US',6]+ i['Recharge','GB',6]+
                    i['Recharge','AU',7]+ i['Recharge','CA',7]+ i['Recharge','US',7]+ i['Recharge','GB',7] )/( i['reg','AU',1]+ i['reg','CA',1]+ i['reg','US',1]+ i['reg','GB',1]) , axis = 1)

And_Cohort_Counrty_Data['Android数据', '1. T1-EN', 'KPI: 7 LTV'] = ''

And_Cohort_Counrty_Data['Android数据', '1. T1-EN', '次留'] = And_Cohort_Counrty_Data.apply( 
        lambda i: ( i['login','AU',2]+ i['login','CA',2]+ i['login','US',2]+ i['login','GB',2])/( i['reg','AU',1]+ i['reg','CA',1]+ i['reg','US',1]+ i['reg','GB',1]) , axis = 1)

## DE 
And_Cohort_Counrty_Data['Android数据', '2. DE', '3 LTV'] = And_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','DE',1]+ i['Recharge','DE',2]+ i['Recharge','DE',3] )/( i['reg','DE',1]) , axis = 1)

And_Cohort_Counrty_Data['Android数据', '2. DE', '7 LTV'] = And_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','DE',1]+ i['Recharge','DE',2]+ i['Recharge','DE',3]+ i['Recharge','DE',4]+ 
                    i['Recharge','DE',5]+ i['Recharge','DE',6]+ i['Recharge','DE',7] )/( i['reg','DE',1]) , axis = 1)

And_Cohort_Counrty_Data['Android数据', '2. DE', 'KPI: 7 LTV'] = ''

And_Cohort_Counrty_Data['Android数据', '2. DE', '次留'] = And_Cohort_Counrty_Data.apply( 
        lambda i: ( i['login','DE',2])/( i['reg','DE',1]) , axis = 1)

## RU
And_Cohort_Counrty_Data['Android数据', '3. RU', '3 LTV'] = And_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','RU',1]+ i['Recharge','RU',2]+ i['Recharge','RU',3] )/( i['reg','RU',1]) , axis = 1)

And_Cohort_Counrty_Data['Android数据', '3. RU', '7 LTV'] = And_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','RU',1]+ i['Recharge','RU',2]+ i['Recharge','RU',3]+ i['Recharge','RU',4]+ 
                    i['Recharge','RU',5]+ i['Recharge','RU',6]+ i['Recharge','RU',7] )/( i['reg','RU',1]) , axis = 1)

And_Cohort_Counrty_Data['Android数据', '3. RU', 'KPI: 7 LTV'] = ''

And_Cohort_Counrty_Data['Android数据', '3. RU', '次留'] = And_Cohort_Counrty_Data.apply( 
        lambda i: ( i['login','RU',2])/( i['reg','RU',1]) , axis = 1)

## HK/TW
And_Cohort_Counrty_Data['Android数据', '4. HK/TW', '3 LTV'] = And_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','HK',1]+ i['Recharge','TW',1]+
                    i['Recharge','HK',2]+ i['Recharge','TW',2]+
                    i['Recharge','HK',3]+ i['Recharge','TW',3])/( i['reg','HK',1]+ i['reg','TW',1]) , axis = 1)

And_Cohort_Counrty_Data['Android数据', '4. HK/TW', '7 LTV'] = And_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','HK',1]+ i['Recharge','TW',1]+
                    i['Recharge','HK',2]+ i['Recharge','TW',2]+
                    i['Recharge','HK',3]+ i['Recharge','TW',3]+
                    i['Recharge','HK',4]+ i['Recharge','TW',4]+
                    i['Recharge','HK',5]+ i['Recharge','TW',5]+
                    i['Recharge','HK',6]+ i['Recharge','TW',6]+
                    i['Recharge','HK',7]+ i['Recharge','TW',7])/( i['reg','HK',1]+ i['reg','TW',1]) , axis = 1)

And_Cohort_Counrty_Data['Android数据', '4. HK/TW', 'KPI: 7 LTV'] = ''

And_Cohort_Counrty_Data['Android数据', '4. HK/TW', '次留'] = And_Cohort_Counrty_Data.apply( 
        lambda i: ( i['login','HK',2]+ i['login','TW',2])/( i['reg','HK',1]+ i['reg','TW',1]) , axis = 1)

## JP/KR
And_Cohort_Counrty_Data['Android数据', '5. JP/KR', '3 LTV'] = And_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','JP',1]+ i['Recharge','KR',1]+
                    i['Recharge','JP',2]+ i['Recharge','KR',2]+
                    i['Recharge','JP',3]+ i['Recharge','KR',3])/( i['reg','JP',1]+ i['reg','KR',1]) , axis = 1)

And_Cohort_Counrty_Data['Android数据', '5. JP/KR', '7 LTV'] = And_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','JP',1]+ i['Recharge','KR',1]+
                    i['Recharge','JP',2]+ i['Recharge','KR',2]+
                    i['Recharge','JP',3]+ i['Recharge','KR',3]+
                    i['Recharge','JP',4]+ i['Recharge','KR',4]+
                    i['Recharge','JP',5]+ i['Recharge','KR',5]+
                    i['Recharge','JP',6]+ i['Recharge','KR',6]+
                    i['Recharge','JP',7]+ i['Recharge','KR',7])/( i['reg','JP',1]+ i['reg','KR',1]) , axis = 1)

And_Cohort_Counrty_Data['Android数据', '5. JP/KR', 'KPI: 7 LTV'] = ''

And_Cohort_Counrty_Data['Android数据', '5. JP/KR', '次留'] = And_Cohort_Counrty_Data.apply( 
        lambda i: ( i['login','JP',2]+ i['login','KR',2])/( i['reg','JP',1]+ i['reg','KR',1]) , axis = 1)

And_Cohort_Counrty_Data = And_Cohort_Counrty_Data.ix[ :, And_Cohort_Counrty_Data.columns.get_level_values(2).isin({"3 LTV", "7 LTV", "KPI: 7 LTV", "次留"})]


## iOS 市场周数据
iOS_Cohort_Counrty = str(
    '''
        SELECT 
            a.Date, a.registerDate, a.xday, registerAreaCode,
            sum( a.loginCnt1) as login, sum( a.registerCnt1) as reg, 
            round( sum( a.paySum1)*0.49,2) as Recharge 
        FROM
        (
        	SELECT
        		Date, registerDate, DATEDIFF( Date, registerDate) +1 as xday, advertiser, countryGroup, registerAreaCode,
        		IFNULL( loginCnt,0) as loginCnt1, IFNULL( registerCnt,0) as registerCnt1, IFNULL( paySum,0) as paySum1
        	FROM
        		`t_data_track`
        	WHERE
        		registerDate BETWEEN '%s' AND '%s'
        	AND Date BETWEEN '%s' AND '%s'
        	AND advertiser != 'untrusted devices'  ## 假量
          AND platform = 'iOS'
        	AND gameCode = 'stgl'
          AND tier = 'T1'
        ) a
        GROUP BY a.Date, a.registerDate, a.xday, registerAreaCode
        HAVING a.xday <= 7;
''' %( date_calculate()[0], date_calculate()[1], date_calculate()[0], date_calculate()[1])) 

iOS_Cohort_Counrty_Data = pd.read_sql( iOS_Cohort_Counrty, con = connection)
iOS_Cohort_Counrty_Data = pd.merge( iOS_Cohort_Counrty_Data, Week_config, left_on = 'registerDate', right_on = 'Date')

iOS_Cohort_Counrty_Data = pd.pivot_table( iOS_Cohort_Counrty_Data, index = ['Class'], columns=['registerAreaCode', 'xday'], values = ['reg','login', 'Recharge'])

iOS_Cohort_Counrty_Data = iOS_Cohort_Counrty_Data.fillna(0)


## T1 -EN 
iOS_Cohort_Counrty_Data['iOS数据', '1. T1-EN', '3 LTV'] = iOS_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','AU',1]+ i['Recharge','CA',1]+ i['Recharge','US',1]+ i['Recharge','GB',1]+
                    i['Recharge','AU',2]+ i['Recharge','CA',2]+ i['Recharge','US',2]+ i['Recharge','GB',2]+
                    i['Recharge','AU',3]+ i['Recharge','CA',3]+ i['Recharge','US',3]+ i['Recharge','GB',3])/( i['reg','AU',1]+ i['reg','CA',1]+ i['reg','US',1]+ i['reg','GB',1]) , axis = 1)

iOS_Cohort_Counrty_Data['iOS数据', '1. T1-EN', '7 LTV'] = iOS_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','AU',1]+ i['Recharge','CA',1]+ i['Recharge','US',1]+ i['Recharge','GB',1]+
                    i['Recharge','AU',2]+ i['Recharge','CA',2]+ i['Recharge','US',2]+ i['Recharge','GB',2]+
                    i['Recharge','AU',3]+ i['Recharge','CA',3]+ i['Recharge','US',3]+ i['Recharge','GB',3]+
                    i['Recharge','AU',4]+ i['Recharge','CA',4]+ i['Recharge','US',4]+ i['Recharge','GB',4]+
                    i['Recharge','AU',5]+ i['Recharge','CA',5]+ i['Recharge','US',5]+ i['Recharge','GB',5]+
                    i['Recharge','AU',6]+ i['Recharge','CA',6]+ i['Recharge','US',6]+ i['Recharge','GB',6]+
                    i['Recharge','AU',7]+ i['Recharge','CA',7]+ i['Recharge','US',7]+ i['Recharge','GB',7] )/( i['reg','AU',1]+ i['reg','CA',1]+ i['reg','US',1]+ i['reg','GB',1]) , axis = 1)

iOS_Cohort_Counrty_Data['iOS数据', '1. T1-EN', 'KPI: 7 LTV'] = ''

iOS_Cohort_Counrty_Data['iOS数据', '1. T1-EN', '次留'] = iOS_Cohort_Counrty_Data.apply( 
        lambda i: ( i['login','AU',2]+ i['login','CA',2]+ i['login','US',2]+ i['login','GB',2])/( i['reg','AU',1]+ i['reg','CA',1]+ i['reg','US',1]+ i['reg','GB',1]) , axis = 1)

## DE 
iOS_Cohort_Counrty_Data['iOS数据', '2. DE', '3 LTV'] = iOS_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','DE',1]+ i['Recharge','DE',2]+ i['Recharge','DE',3] )/( i['reg','DE',1]) , axis = 1)

iOS_Cohort_Counrty_Data['iOS数据', '2. DE', '7 LTV'] = iOS_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','DE',1]+ i['Recharge','DE',2]+ i['Recharge','DE',3]+ i['Recharge','DE',4]+ 
                    i['Recharge','DE',5]+ i['Recharge','DE',6]+ i['Recharge','DE',7] )/( i['reg','DE',1]) , axis = 1)

iOS_Cohort_Counrty_Data['iOS数据', '2. DE', 'KPI: 7 LTV'] = ''

iOS_Cohort_Counrty_Data['iOS数据', '2. DE', '次留'] = iOS_Cohort_Counrty_Data.apply( 
        lambda i: ( i['login','DE',2])/( i['reg','DE',1]) , axis = 1)

## RU
iOS_Cohort_Counrty_Data['iOS数据', '3. RU', '3 LTV'] = iOS_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','RU',1]+ i['Recharge','RU',2]+ i['Recharge','RU',3] )/( i['reg','RU',1]) , axis = 1)

iOS_Cohort_Counrty_Data['iOS数据', '3. RU', '7 LTV'] = iOS_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','RU',1]+ i['Recharge','RU',2]+ i['Recharge','RU',3]+ i['Recharge','RU',4]+ 
                    i['Recharge','RU',5]+ i['Recharge','RU',6]+ i['Recharge','RU',7] )/( i['reg','RU',1]) , axis = 1)

iOS_Cohort_Counrty_Data['iOS数据', '3. RU', 'KPI: 7 LTV'] = ''

iOS_Cohort_Counrty_Data['iOS数据', '3. RU', '次留'] = iOS_Cohort_Counrty_Data.apply( 
        lambda i: ( i['login','RU',2])/( i['reg','RU',1]) , axis = 1)

## HK/TW
iOS_Cohort_Counrty_Data['iOS数据', '4. HK/TW', '3 LTV'] = iOS_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','HK',1]+ i['Recharge','TW',1]+
                    i['Recharge','HK',2]+ i['Recharge','TW',2]+
                    i['Recharge','HK',3]+ i['Recharge','TW',3])/( i['reg','HK',1]+ i['reg','TW',1]) , axis = 1)

iOS_Cohort_Counrty_Data['iOS数据', '4. HK/TW', '7 LTV'] = iOS_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','HK',1]+ i['Recharge','TW',1]+
                    i['Recharge','HK',2]+ i['Recharge','TW',2]+
                    i['Recharge','HK',3]+ i['Recharge','TW',3]+
                    i['Recharge','HK',4]+ i['Recharge','TW',4]+
                    i['Recharge','HK',5]+ i['Recharge','TW',5]+
                    i['Recharge','HK',6]+ i['Recharge','TW',6]+
                    i['Recharge','HK',7]+ i['Recharge','TW',7])/( i['reg','HK',1]+ i['reg','TW',1]) , axis = 1)

iOS_Cohort_Counrty_Data['iOS数据', '4. HK/TW', 'KPI: 7 LTV'] = ''

iOS_Cohort_Counrty_Data['iOS数据', '4. HK/TW', '次留'] = iOS_Cohort_Counrty_Data.apply( 
        lambda i: ( i['login','HK',2]+ i['login','TW',2])/( i['reg','JP',1]+ i['reg','KR',1]) , axis = 1)

## JP/KR
iOS_Cohort_Counrty_Data['iOS数据', '5. JP/KR', '3 LTV'] = iOS_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','JP',1]+ i['Recharge','KR',1]+
                    i['Recharge','JP',2]+ i['Recharge','KR',2]+
                    i['Recharge','JP',3]+ i['Recharge','KR',3])/( i['reg','JP',1]+ i['reg','KR',1]) , axis = 1)

iOS_Cohort_Counrty_Data['iOS数据', '5. JP/KR', '7 LTV'] = iOS_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','JP',1]+ i['Recharge','KR',1]+
                    i['Recharge','JP',2]+ i['Recharge','KR',2]+
                    i['Recharge','JP',3]+ i['Recharge','KR',3]+
                    i['Recharge','JP',4]+ i['Recharge','KR',4]+
                    i['Recharge','JP',5]+ i['Recharge','KR',5]+
                    i['Recharge','JP',6]+ i['Recharge','KR',6]+
                    i['Recharge','JP',7]+ i['Recharge','KR',7])/( i['reg','JP',1]+ i['reg','KR',1]) , axis = 1)

iOS_Cohort_Counrty_Data['iOS数据', '5. JP/KR', 'KPI: 7 LTV'] = ''

iOS_Cohort_Counrty_Data['iOS数据', '5. JP/KR', '次留'] = iOS_Cohort_Counrty_Data.apply( 
        lambda i: ( i['login','JP',2]+ i['login','KR',2])/( i['reg','JP',1]+ i['reg','KR',1]) , axis = 1)

iOS_Cohort_Counrty_Data = iOS_Cohort_Counrty_Data.ix[ :, iOS_Cohort_Counrty_Data.columns.get_level_values(2).isin({"3 LTV", "7 LTV", "KPI: 7 LTV", "次留"})]

## ------ 早会数据 ------
Meeting = pd.DataFrame( columns =  ['Date', 'Recharge', 'Pay_Rate', 'ARPPU',  'DNU', 'DAU', 'EDAU',
                                    '安卓次留', 'iOS次留', '1自然量LTV', '3LTV',  '7LTV', 'CPU', 'COST',
                                    '3ROI', '7ROI', 'And_Cost', 'iOS_Cost',  'Pay_Num'])
## 运营数据
Meeting['Date'] = And_actual_data[['Date']]
Meeting['Recharge'] = And_actual_data['Recharge'] + iOS_actual_data['Recharge']
Meeting['DNU'] = And_actual_data['DNU'] + iOS_actual_data['DNU']
Meeting['DAU'] = And_actual_data['DAU'] + iOS_actual_data['DAU']
Meeting['Pay_Num'] = And_actual_data['Pay_Num'] + iOS_actual_data['Pay_Num']
## 留存
Meeting['安卓次留'] = pd.DataFrame( list( And_Cohort_Data['留存', '次留']))
Meeting['iOS次留'] = pd.DataFrame( list( iOS_Cohort_Data['留存', '次留']))
## LTV
Meeting['1自然量LTV'] = pd.DataFrame( 
        list( 
                Whole_Cohoe_Data.apply( lambda i: (i['Recharge', '自然量', 1])/i['reg', '自然量', 1] , axis = 1)
                )
        )
Meeting['3LTV'] = pd.DataFrame( list( Whole_Cohoe_Data['3LTV', '整体', '3 LTV1']))
Meeting['7LTV'] = pd.DataFrame( list( Whole_Cohoe_Data['7LTV', '整体', '7 LTV1']))


## ------ Output ------
## Operation
output = str( r"C:\Users\Efun\Desktop\日报基础数据（产品） - %s.xlsx" % (date_calculate()[1]) )
writer = pd.ExcelWriter( output)

And_actual_data.to_excel( writer, index = False, encoding = 'utf-8', sheet_name = 'And_actual')
iOS_actual_data.to_excel( writer, index = False, encoding = 'utf-8', sheet_name = 'iOS_actual')
And.to_excel( writer, encoding = 'utf-8', sheet_name = 'And_dnu')
iOS.to_excel( writer, encoding = 'utf-8', sheet_name = 'iOS_dnu')
And_Cohort_Data_Op.to_excel( writer, encoding = 'utf-8', sheet_name = 'And_Cohort')
iOS_Cohort_Data_Op.to_excel( writer, encoding = 'utf-8', sheet_name = 'iOS_Cohort')
Whole_Cohoe_Data1.to_excel( writer, encoding = 'utf-8', sheet_name = '整体留存')
Whole_Cohoe_Data2.to_excel( writer, encoding = 'utf-8', sheet_name = '整体LTV')
Meeting.to_excel( writer, index = False, encoding = 'utf-8', sheet_name = '早会数据')

writer.save()

## Market
output = str( r"C:\Users\Efun\Desktop\日报基础数据（市场） - %s.xlsx" % (date_calculate()[1]) )
writer = pd.ExcelWriter( output)

And_Cohort_Data_Market.to_excel( writer, encoding = 'utf-8', sheet_name = '日报-Android')
iOS_Cohort_Data_Market.to_excel( writer, encoding = 'utf-8', sheet_name = '日报-iOS')
And_Market.to_excel( writer, encoding = 'utf-8', sheet_name = 'DNU监控-Android')
iOS_Market.to_excel( writer, encoding = 'utf-8', sheet_name = 'DNU监控-iOS')
And_Cohort_Counrty_Data.to_excel( writer, encoding = 'utf-8', sheet_name = 'LTV监控-分地区-Android')
iOS_Cohort_Counrty_Data.to_excel( writer, encoding = 'utf-8', sheet_name = 'LTV监控-分地区-iOS')

writer.save()
"""
Created on Thu Jun 28 17:39:44 2018

@author: Efun
"""

## 报表自动化

import pandas as pd
import MySQLdb as mdb
import datetime
import time
import copy

connection = mdb.connect( host = '210.59.244.161',
                          user = 'efun_select',
                          password = 'uPOX8bl+z%Q;EkA',
                          db = 'efun_global',
                          charset = 'utf8')


def date_calculate( n1 = 21, n2 = 1):
    '''
        n1: 用于计算开始时间
        n2: 用于计算结束时间, 即昨天的日期。
    '''
    today = datetime.date.today() 
    start_date = today - datetime.timedelta( days = n1)  
    end_date = today - datetime.timedelta( days = n2)  
    return( start_date.strftime( '%Y-%m-%d'), end_date.strftime( '%Y-%m-%d') )
    
def weeknum( date = datetime.date.today().strftime( '%Y-%m-%d') ):
    '''
        字符串日期转周几
    ''' 
    return( time.strptime( date,"%Y-%m-%d").tm_wday + 1)
    
    
    
    
## ------ Actual Data ------
## And Actual Data
And_actual_str = str(
    '''
    SELECT
    	Date,
       "" as Week,
    	ROUND( SUM( paySum),2) as Recharge,
       "" as Cost, 
       SUM( registerCnt) as DNU,
       "" as CPU, 
      	SUM( loginCnt) as DAU,
       "" as EDAU, 
    	SUM( payCnt) as Pay_Num 
    FROM
    	`t_data_track`
    WHERE
    	Date BETWEEN '%s' AND '%s'
    AND advertiser != 'untrusted devices'   ## 去除假量
    AND platform = 'android'
    AND gameCode = 'stgl'
    GROUP BY Date
    ORDER BY Date;
''' %( date_calculate()[0], date_calculate()[1])) 
    

And_actual_data = pd.read_sql( And_actual_str, con = connection)
And_actual_data.loc[ :, "Week"] = And_actual_data.apply( 
            lambda i: weeknum( date = str( i[ "Date"])), axis = 1)

And_actual_data[ "Week"] = And_actual_data.apply( lambda i: weeknum( str( i['Date'])) , axis = 1)

## iOS Actual Data
iOS_actual_str = str(
    '''
    SELECT
    	Date,
       "" as Week,
    	ROUND( SUM( paySum),2) as Recharge,
       "" as Cost, 
       SUM( registerCnt) as DNU,
       "" as CPU, 
      	SUM( loginCnt) as DAU,
       "" as EDAU, 
    	SUM( payCnt) as Pay_Num 
    FROM
    	`t_data_track`
    WHERE
    	Date BETWEEN '%s' AND '%s'
    AND advertiser != 'untrusted devices'   ## 去除假量
    AND platform = 'ios'
    AND gameCode = 'stgl'
    GROUP BY Date
    ORDER BY Date;
''' %( date_calculate()[0], date_calculate()[1])) 
    

iOS_actual_data = pd.read_sql( iOS_actual_str, con = connection)
iOS_actual_data.loc[ :, "Week"] = iOS_actual_data.apply( 
            lambda i: weeknum( date = str( i[ "Date"])), axis = 1)


## ------ DNU 占比 ------
Index = pd.DataFrame({
        'Index1': ["T1", "ROW", "AU", "CA", "US", "GB", "DE", "FR", "RU", "TW", "HK", "JP", "KR"],
        'Index': ["01.T1", "02.ROW", "03.AU", "04.CA", "05.US", "06.GB", "07.DE", "08.FR", "09.RU", "10.TW", "11.HK", "12.JP", "13.KR"]
        })
## "CH", "AT", "CN", "MO" 剔除, 方面下面做 Merge时直接剔除这四个地区
    
And_Country_Dnu_str = str(
    '''
        SELECT
        	Date,
        	registerAreaCode as Index1,
        	SUM( IFNULL( registerCnt,0)) AS DNU
        FROM
        	`t_data_track`
        WHERE
        	Date BETWEEN '%s' AND '%s'
        AND advertiser != 'untrusted devices' ## 去除假量
        AND platform = 'android'
        AND gameCode = 'stgl'
        AND tier = 'T1'
        GROUP BY
        	Date, Index1, tier
        ORDER BY
        	Date;
''' %( date_calculate()[0], date_calculate()[1])) 
    
And_Tier_Dnu_str = str(
    '''
        SELECT
        	Date,
        	CASE 
        		WHEN tier = 'T1' THEN 'T1'
        		WHEN tier <> 'T1' THEN 'ROW'
        	END AS Index1,
        	SUM( IFNULL( registerCnt,0)) AS DNU
        FROM
        	`t_data_track`
        WHERE
        	Date BETWEEN '%s' AND '%s'
        AND advertiser != 'untrusted devices' ## 去除假量
        AND platform = 'android'
        AND gameCode = 'stgl'
        GROUP BY
        	Date, Index1
        ORDER BY
        	Date;
''' %( date_calculate()[0], date_calculate()[1])) 
    

And_Country_Dnu_data = pd.read_sql( And_Country_Dnu_str, con = connection)
And_Tier_Dnu_str_data = pd.read_sql( And_Tier_Dnu_str, con = connection)
And_Country_Dnu_data = pd.concat( [ And_Country_Dnu_data, And_Tier_Dnu_str_data])
And_Country_Dnu_data = pd.merge( And_Country_Dnu_data, Index, on = 'Index1')

And = pd.pivot_table( And_Country_Dnu_data, index = ['Date'], columns=['Index'], values = ['DNU'])
And = And.fillna(0)
And['DNU', '00.DNU'] = And.apply( lambda i: i['DNU', "01.T1"] + i['DNU', "02.ROW"] , axis = 1)
And = And.sort_index( axis = 1)
# And.ix[ :, And.columns.get_level_values(1).isin({"00.DNU", "01.T1", "02.ROW", "03.AU", "04.CA", "05.US", "06.GB", "07.DE", "08.FR", "09.RU", "10.TW", "11.HK", "12.JP", "13.KR"})]

And_Market = copy.deepcopy( And)
And_Market['DNU', '1.T1-EN'] = And_Market.apply( lambda i:  i['DNU', "03.AU"] + i['DNU', "04.CA"] + i['DNU', "05.US"] + i['DNU', "06.GB"] , axis = 1)
And_Market['DNU', '2.DE'] = And_Market.apply( lambda i:  i['DNU', "07.DE"] , axis = 1)
And_Market['DNU', '3.RU'] = And_Market.apply( lambda i:  i['DNU', "09.RU"] , axis = 1)
And_Market['DNU', '4.HK/TW/JP/KR'] = And_Market.apply( lambda i:  i['DNU', "10.TW"] + i['DNU', "11.HK"] + i['DNU', "12.JP"] + i['DNU', "13.KR"] , axis = 1)
And_Market['DNU', '5.ROW'] = And_Market.apply( lambda i:  i['DNU', "02.ROW"] , axis = 1)

And_Market = And_Market.ix[ :, And_Market.columns.get_level_values(1).isin({"1.T1-EN", "2.DE", "3.RU", "4.HK/TW/JP/KR", "5.ROW"})]


iOS_Country_Dnu_str = str(
    '''
        SELECT
        	Date,
        	registerAreaCode as Index1,
        	SUM( IFNULL( registerCnt,0)) AS DNU
        FROM
        	`t_data_track`
        WHERE
        	Date BETWEEN '%s' AND '%s'
        AND advertiser != 'untrusted devices' ## 去除假量
        AND platform = 'ios'
        AND gameCode = 'stgl'
        AND tier = 'T1'
        GROUP BY
        	Date, Index1, tier
        ORDER BY
        	Date;
''' %( date_calculate()[0], date_calculate()[1])) 
    
iOS_Tier_Dnu_str = str(
    '''
        SELECT
        	Date,
        	CASE 
        		WHEN tier = 'T1' THEN 'T1'
        		WHEN tier <> 'T1' THEN 'ROW'
        	END AS Index1,
        	SUM( IFNULL( registerCnt,0)) AS DNU
        FROM
        	`t_data_track`
        WHERE
        	Date BETWEEN '%s' AND '%s'
        AND advertiser != 'untrusted devices' ## 去除假量
        AND platform = 'iOS'
        AND gameCode = 'stgl'
        GROUP BY
        	Date, Index1
        ORDER BY
        	Date;
''' %( date_calculate()[0], date_calculate()[1])) 
    

iOS_Country_Dnu_data = pd.read_sql( iOS_Country_Dnu_str, con = connection)
iOS_Tier_Dnu_str_data = pd.read_sql( iOS_Tier_Dnu_str, con = connection)
iOS_Country_Dnu_data = pd.concat( [ iOS_Country_Dnu_data, iOS_Tier_Dnu_str_data])
iOS_Country_Dnu_data = pd.merge( iOS_Country_Dnu_data, Index, on = 'Index1')

iOS = pd.pivot_table( iOS_Country_Dnu_data, index = ['Date'], columns=['Index'], values = ['DNU'])
iOS = iOS.fillna(0)
iOS['DNU', '00.DNU'] = iOS.apply( lambda i: i['DNU', "01.T1"] + i['DNU', "02.ROW"] , axis = 1)
iOS = iOS.sort_index( axis = 1)
#iOS.ix[ :, iOS.columns.get_level_values(1).isin({"01.T1", "02.ROW", "03.AU", "04.CA", "05.US", "06.GB", "07.DE", "08.FR", "09.RU", "10.TW", "11.HK", "12.JP", "13.KR"})]

iOS_Market = copy.deepcopy( iOS)
iOS_Market['DNU', '1.T1-EN'] = iOS_Market.apply( lambda i:  i['DNU', "03.AU"] + i['DNU', "04.CA"] + i['DNU', "05.US"] + i['DNU', "06.GB"] , axis = 1)
iOS_Market['DNU', '2.DE'] = iOS_Market.apply( lambda i:  i['DNU', "07.DE"] , axis = 1)
iOS_Market['DNU', '3.RU'] = iOS_Market.apply( lambda i:  i['DNU', "09.RU"] , axis = 1)
iOS_Market['DNU', '4.HK/TW/JP/KR'] = iOS_Market.apply( lambda i:  i['DNU', "10.TW"] + i['DNU', "11.HK"] + i['DNU', "12.JP"] + i['DNU', "13.KR"] , axis = 1)
iOS_Market['DNU', '5.ROW'] = iOS_Market.apply( lambda i:  i['DNU', "02.ROW"] , axis = 1)

iOS_Market = iOS_Market.ix[ :, iOS_Market.columns.get_level_values(1).isin({"1.T1-EN", "2.DE", "3.RU", "4.HK/TW/JP/KR", "5.ROW"})]


## ------ 留存Cohort ------
And_Cohort = str(
    '''
        SELECT 
            a.Date, a.registerDate, a.xday, 
            sum( a.loginCnt1) as login, sum( a.registerCnt1) as reg, 
            round( sum( a.paySum1)*0.49,2) as Recharge 
        FROM
        (
        	SELECT
        		Date, registerDate, DATEDIFF( Date, registerDate) +1 as xday, advertiser, countryGroup, 
        		IFNULL( loginCnt,0) as loginCnt1, IFNULL( registerCnt,0) as registerCnt1, IFNULL( paySum,0) as paySum1
        	FROM
        		`t_data_track`
        	WHERE
        		registerDate BETWEEN '%s' AND '%s'
        	AND Date BETWEEN '%s' AND '%s'
        	AND advertiser != 'untrusted devices'  ## 假量
          AND platform = 'android'
        	AND gameCode = 'stgl'
        ) a
        GROUP BY a.Date, a.registerDate, a.xday
        HAVING a.xday <= 7;
''' %( date_calculate()[0], date_calculate()[1], date_calculate()[0], date_calculate()[1])) 

And_Cohort_Data = pd.read_sql( And_Cohort, con = connection)
And_Cohort_Data = pd.pivot_table( And_Cohort_Data, index = ['registerDate'], columns=['xday'], values = ['reg','login', 'Recharge'])
#And_Cohort_Data = And_Cohort_Data.fillna(0)

And_Cohort_Data['留存', 'DNU'] = And_Cohort_Data['reg', 1]
And_Cohort_Data['留存', '次留'] = And_Cohort_Data.apply( lambda i: i['login', 2]/i['reg', 1] , axis = 1)
And_Cohort_Data['留存', '7留'] = And_Cohort_Data.apply( lambda i: i['login', 7]/i['reg', 1] , axis = 1)
And_Cohort_Data['留存', 'CPU'] = ''

And_Cohort_Data['LTV', '1 LTV'] = And_Cohort_Data.apply( 
        lambda i: i['Recharge', 1]/i['reg', 1] , axis = 1)
And_Cohort_Data['LTV', '1 ROI'] = ''
And_Cohort_Data['LTV', '3 LTV'] = And_Cohort_Data.apply( 
        lambda i: '-' if np.isnan( i['Recharge', 3]) else (i['Recharge', 1]+i['Recharge', 2]+i['Recharge', 3])/i['reg', 1] , axis = 1)
And_Cohort_Data['LTV', '3 ROI'] = ''
And_Cohort_Data['LTV', '7 LTV'] = And_Cohort_Data.apply( 
        lambda i: '-' if np.isnan( i['Recharge', 7]) else (i['Recharge', 1]+i['Recharge', 2]+i['Recharge', 3]+i['Recharge', 4]+i['Recharge', 5]+i['Recharge', 6]+i['Recharge', 7])/i['reg', 1] , axis = 1)


And_Cohort_Data_Op = And_Cohort_Data.ix[ :, And_Cohort_Data.columns.get_level_values(1).isin({"DNU", "次留", "7留", "CPU", "3 LTV", "3 ROI", "7 LTV"})]
## 市场日报
And_Cohort_Data_Market = And_Cohort_Data.ix[ :, And_Cohort_Data.columns.get_level_values(1).isin({"DNU", "次留", "1 LTV", "1 ROI", "3 LTV", "3 ROI", "7 LTV"})]  


iOS_Cohort = str(
    '''
        SELECT 
            a.Date, a.registerDate, a.xday, 
            sum( a.loginCnt1) as login, sum( a.registerCnt1) as reg, 
            round( sum( a.paySum1)*0.49,2) as Recharge 
        FROM
        (
        	SELECT
        		Date, registerDate, DATEDIFF( Date, registerDate) +1 as xday, advertiser, countryGroup, 
        		IFNULL( loginCnt,0) as loginCnt1, IFNULL( registerCnt,0) as registerCnt1, IFNULL( paySum,0) as paySum1
        	FROM
        		`t_data_track`
        	WHERE
        		registerDate BETWEEN '%s' AND '%s'
        	AND Date BETWEEN '%s' AND '%s'
        	AND advertiser != 'untrusted devices'  ## 假量
          AND platform = 'ios'
        	AND gameCode = 'stgl'
        ) a
        GROUP BY a.Date, a.registerDate, a.xday
        HAVING a.xday <= 7;
''' %( date_calculate()[0], date_calculate()[1], date_calculate()[0], date_calculate()[1])) 

iOS_Cohort_Data = pd.read_sql( iOS_Cohort, con = connection)
iOS_Cohort_Data = pd.pivot_table( iOS_Cohort_Data, index = ['registerDate'], columns=['xday'], values = ['reg','login', 'Recharge'])
#iOS_Cohort_Data = iOS_Cohort_Data.fillna(0)

iOS_Cohort_Data['留存', 'DNU'] = iOS_Cohort_Data['reg', 1]
iOS_Cohort_Data['留存', '次留'] = iOS_Cohort_Data.apply( lambda i: i['login', 2]/i['reg', 1] , axis = 1)
iOS_Cohort_Data['留存', '7留'] = iOS_Cohort_Data.apply( lambda i: i['login', 7]/i['reg', 1] , axis = 1)
iOS_Cohort_Data['留存', 'CPU'] = ''

iOS_Cohort_Data['LTV', '1 LTV'] = iOS_Cohort_Data.apply( 
        lambda i: i['Recharge', 1]/i['reg', 1] , axis = 1)
iOS_Cohort_Data['LTV', '1 ROI'] = ''
iOS_Cohort_Data['LTV', '3 LTV'] = iOS_Cohort_Data.apply( 
        lambda i: '-' if np.isnan( i['Recharge', 3]) else (i['Recharge', 1]+i['Recharge', 2]+i['Recharge', 3])/i['reg', 1] , axis = 1)
iOS_Cohort_Data['LTV', '3 ROI'] = ''
iOS_Cohort_Data['LTV', '7 LTV'] = iOS_Cohort_Data.apply( 
        lambda i: '-' if np.isnan( i['Recharge', 7]) else (i['Recharge', 1]+i['Recharge', 2]+i['Recharge', 3]+i['Recharge', 4]+i['Recharge', 5]+i['Recharge', 6]+i['Recharge', 7])/i['reg', 1] , axis = 1)

iOS_Cohort_Data_Op = iOS_Cohort_Data.ix[ :, iOS_Cohort_Data.columns.get_level_values(1).isin({"DNU", "次留", "7留", "CPU", "3 LTV", "3 ROI", "7 LTV"})]
## 市场日报
iOS_Cohort_Data_Market = iOS_Cohort_Data.ix[ :, iOS_Cohort_Data.columns.get_level_values(1).isin({"DNU", "次留", "1 LTV", "1 ROI", "3 LTV", "3 ROI", "7 LTV"})]  


Whole_Cohor = str(
    '''
        SELECT 
            a.Date, "整体" as Type, a.registerDate, a.xday, 
            sum( a.loginCnt1) as login, sum( a.registerCnt1) as reg, round( sum( a.paySum1)*0.49,2) as Recharge 
        FROM
        (
        	SELECT
        		Date, registerDate, DATEDIFF( Date, registerDate) +1 as xday, advertiser, countryGroup, 
        		IFNULL( loginCnt,0) as loginCnt1, IFNULL( registerCnt,0) as registerCnt1, IFNULL( paySum,0) as paySum1
        	FROM
        		`t_data_track`
        	WHERE
        		registerDate BETWEEN '%s' AND '%s'
        	AND Date BETWEEN '%s' AND '%s'
        	AND advertiser != 'untrusted devices'  ## 假量
        	AND gameCode = 'stgl'
        ) a
        WHERE a.xday <= 7
        GROUP BY a.Date, a.registerDate, a.xday
        UNION
        SELECT 
            a.Date, "自然量" as Type, a.registerDate, a.xday, 
            sum( a.loginCnt1) as login, sum( a.registerCnt1) as reg, round( sum( a.paySum1)*0.49,2) as Recharge 
        FROM
        (
        	SELECT
        		Date, registerDate, DATEDIFF( Date, registerDate) +1 as xday, advertiser, countryGroup, 
        		IFNULL( loginCnt,0) as loginCnt1, IFNULL( registerCnt,0) as registerCnt1, IFNULL( paySum,0) as paySum1
        	FROM
        		`t_data_track`
        	WHERE
        		registerDate BETWEEN '%s' AND '%s'
        	AND Date BETWEEN '%s' AND '%s'
        	AND advertiser != 'untrusted devices'  ## 假量
        	AND advertiser = 'organic'
        	AND gameCode = 'stgl'
        ) a
        WHERE a.xday <= 7
        GROUP BY a.Date, a.registerDate, a.xday
        UNION
        SELECT 
            a.Date, "渠道量" as Type, a.registerDate, a.xday, 
            sum( a.loginCnt1) as login, sum( a.registerCnt1) as reg, round( sum( a.paySum1)*0.49,2) as Recharge  
        FROM
        (
        	SELECT
        		Date, registerDate, DATEDIFF( Date, registerDate) +1 as xday, advertiser, countryGroup, 
        		IFNULL( loginCnt,0) as loginCnt1, IFNULL( registerCnt,0) as registerCnt1, IFNULL( paySum,0) as paySum1
        	FROM
        		`t_data_track`
        	WHERE
        		registerDate BETWEEN '%s' AND '%s'
        	AND Date BETWEEN '%s' AND '%s'
        	AND advertiser != 'untrusted devices'  ## 假量
        	AND advertiser != 'organic'
        	AND gameCode = 'stgl'
        ) a
        WHERE a.xday <= 7
        GROUP BY a.Date, a.registerDate, a.xday;
''' %( date_calculate()[0], date_calculate()[1], date_calculate()[0], date_calculate()[1],
        date_calculate()[0], date_calculate()[1], date_calculate()[0], date_calculate()[1],
        date_calculate()[0], date_calculate()[1], date_calculate()[0], date_calculate()[1])) 

Whole_Cohoe_Data = pd.read_sql( Whole_Cohor, con = connection)
Whole_Cohoe_Data = pd.pivot_table( Whole_Cohoe_Data, index = ['registerDate'], columns=['Type','xday'], values = ['reg','login', 'Recharge'])
# Whole_Cohoe_Data = Whole_Cohoe_Data.fillna(0)

## ------ 整体留存 ------
Whole_Cohoe_Data['留存', '整体', 'DNU'] = Whole_Cohoe_Data['reg', '整体', 1]
Whole_Cohoe_Data['次日留存率', '整体', '次留1'] = Whole_Cohoe_Data.apply( lambda i: i['login', '整体', 2]/i['reg', '整体', 1] , axis = 1)
Whole_Cohoe_Data['次日留存率', '自然量', '次留2'] = Whole_Cohoe_Data.apply( lambda i: i['login', '自然量', 2]/i['reg', '自然量', 1] , axis = 1)
Whole_Cohoe_Data['次日留存率', '渠道量', '次留3'] = Whole_Cohoe_Data.apply( lambda i: i['login', '渠道量', 2]/i['reg', '渠道量', 1] , axis = 1)
Whole_Cohoe_Data['次日留存率', '整体', '7留1'] = Whole_Cohoe_Data.apply( lambda i: i['login', '整体', 7]/i['reg', '整体', 1] , axis = 1)
Whole_Cohoe_Data['次日留存率', '自然量', '7留2'] = Whole_Cohoe_Data.apply( lambda i: i['login', '自然量', 7]/i['reg', '自然量', 1] , axis = 1)
Whole_Cohoe_Data['次日留存率', '渠道量', '7留3'] = Whole_Cohoe_Data.apply( lambda i: i['login', '渠道量', 7]/i['reg', '渠道量', 1] , axis = 1)

Whole_Cohoe_Data1 = Whole_Cohoe_Data.ix[ :, Whole_Cohoe_Data.columns.get_level_values(2).isin({"DNU", "次留1", "次留2", "次留3", "7留1", "7留2", "7留3"})]


## ------ 整体LTV ------
Whole_Cohoe_Data['LTV', '整体', '1 ROI'] = ''
Whole_Cohoe_Data['LTV', '整体', '3 ROI'] = ''
Whole_Cohoe_Data['LTV', '整体', '7 ROI'] = ''
Whole_Cohoe_Data['LTV', '整体', '7ROI/3ROI'] = ''

Whole_Cohoe_Data['LTV', '整体', '1 LTV'] = Whole_Cohoe_Data.apply( 
        lambda i: (i['Recharge', '整体', 1])/i['reg', '整体', 1] , axis = 1)
Whole_Cohoe_Data['3LTV', '整体', '3 LTV1'] = Whole_Cohoe_Data.apply( 
        lambda i: '-' if np.isnan( i['Recharge', '整体', 3]) else (i['Recharge', '整体', 1]+i['Recharge', '整体', 2]+i['Recharge', '整体', 3])/i['reg', '整体', 1] , axis = 1)
Whole_Cohoe_Data['3LTV', '自然量', '3 LTV2'] = Whole_Cohoe_Data.apply( 
        lambda i: '-' if np.isnan( i['Recharge', '自然量', 3]) else (i['Recharge', '自然量', 1]+i['Recharge', '自然量', 2]+i['Recharge', '自然量', 3])/i['reg', '自然量', 1] , axis = 1)
Whole_Cohoe_Data['3LTV', '渠道量', '3 LTV3'] = Whole_Cohoe_Data.apply( 
        lambda i: '-' if np.isnan( i['Recharge', '渠道量', 3]) else (i['Recharge', '渠道量', 1]+i['Recharge', '渠道量', 2]+i['Recharge', '渠道量', 3])/i['reg', '渠道量', 1] , axis = 1)

Whole_Cohoe_Data['7LTV', '整体', '7 LTV1'] = Whole_Cohoe_Data.apply( 
        lambda i: '-' if np.isnan( i['Recharge', '整体', 7]) else (i['Recharge', '整体', 1]+i['Recharge', '整体', 2]+i['Recharge', '整体', 3]+i['Recharge', '整体', 4]+i['Recharge', '整体', 5]+i['Recharge', '整体', 6]+i['Recharge', '整体', 7])/i['reg', '整体', 1] , axis = 1)
Whole_Cohoe_Data['7LTV', '自然量', '7 LTV2'] = Whole_Cohoe_Data.apply( 
        lambda i: '-' if np.isnan( i['Recharge', '自然量', 7]) else (i['Recharge', '自然量', 1]+i['Recharge', '自然量', 2]+i['Recharge', '自然量', 3]+i['Recharge', '自然量', 4]+i['Recharge', '自然量', 5]+i['Recharge', '自然量', 6]+i['Recharge', '自然量', 7])/i['reg', '自然量', 1] , axis = 1)
Whole_Cohoe_Data['7LTV', '渠道量', '7 LTV3'] = Whole_Cohoe_Data.apply( 
        lambda i: '-' if np.isnan( i['Recharge', '渠道量', 7]) else (i['Recharge', '渠道量', 1]+i['Recharge', '渠道量', 2]+i['Recharge', '渠道量', 3]+i['Recharge', '渠道量', 4]+i['Recharge', '渠道量', 5]+i['Recharge', '渠道量', 6]+i['Recharge', '渠道量', 7])/i['reg', '渠道量', 1] , axis = 1)

Whole_Cohoe_Data2 = Whole_Cohoe_Data.ix[ :, Whole_Cohoe_Data.columns.get_level_values(2).isin({"DNU", "1 ROI", "3 ROI", "7 ROI", "7ROI/3ROI", "1 LTV", "3 LTV1", "3 LTV2", "3 LTV3", "7 LTV1", "7 LTV2", "7 LTV3"})]


## ------ DNU分生命周期 ------
## 登录表查询太慢了, 暂时不放在这个脚本中；


## ------ Marketing ------
def make_config():
    Week_config = copy.deepcopy( And_actual_data.loc[ :,"Date": "Week"])
    Week_config['Temp'] = Week_config.apply( 
            lambda i: '3天' if i['Week'] >= 3 and i['Week'] <= 5 else '4天', axis = 1)
    
    Week_config['Temp1'] = 1 
    for i in range( 1, len( Week_config)):
        if Week_config.loc[ i, 'Temp'] == Week_config.loc[ i-1, 'Temp']:
            Week_config.loc[ i, 'Temp1'] = Week_config.loc[ i-1, 'Temp1'] 
        else:
            Week_config.loc[ i, 'Temp1'] = Week_config.loc[ i-1, 'Temp1'] + 1
            
    Week_config1 = pd.DataFrame({ 'Temp1': list( range( min( Week_config['Temp1']), max( Week_config['Temp1']+1) ) ),
                                'Class' : '',
                                'Day_num' : ''})
    
    for i in Week_config1['Temp1']:
        Week_config1.loc[ i-1, 'Class'] = str( 
                str( i) + ". " +
                min( Week_config[ Week_config['Temp1'] == i]['Date']).strftime( '%m.%d') + 
                "-" + 
                max( Week_config[ Week_config['Temp1'] == i]['Date']).strftime( '%m.%d')
                )
        Week_config1.loc[ i-1, 'Day_num'] = len( Week_config[ Week_config['Temp1'] == i])
        
    Week_config = pd.merge( Week_config, Week_config1, on = 'Temp1')
    del Week_config1
    return( Week_config.loc[ :,['Date', 'Temp', 'Class', 'Day_num']])

Week_config = make_config()


## Android 市场周数据
And_Cohort_Counrty = str(
    '''
        SELECT 
            a.Date, a.registerDate, a.xday, registerAreaCode,
            sum( a.loginCnt1) as login, sum( a.registerCnt1) as reg, 
            round( sum( a.paySum1)*0.49,2) as Recharge 
        FROM
        (
        	SELECT
        		Date, registerDate, DATEDIFF( Date, registerDate) +1 as xday, advertiser, countryGroup, registerAreaCode,
        		IFNULL( loginCnt,0) as loginCnt1, IFNULL( registerCnt,0) as registerCnt1, IFNULL( paySum,0) as paySum1
        	FROM
        		`t_data_track`
        	WHERE
        		registerDate BETWEEN '%s' AND '%s'
        	AND Date BETWEEN '%s' AND '%s'
        	AND advertiser != 'untrusted devices'  ## 假量
          AND platform = 'android'
        	AND gameCode = 'stgl'
          AND tier = 'T1'
        ) a
        GROUP BY a.Date, a.registerDate, a.xday, registerAreaCode
        HAVING a.xday <= 7;
''' %( date_calculate()[0], date_calculate()[1], date_calculate()[0], date_calculate()[1])) 

And_Cohort_Counrty_Data = pd.read_sql( And_Cohort_Counrty, con = connection)
And_Cohort_Counrty_Data = pd.merge( And_Cohort_Counrty_Data, Week_config, left_on = 'registerDate', right_on = 'Date')

And_Cohort_Counrty_Data = pd.pivot_table( And_Cohort_Counrty_Data, index = ['Class'], columns=['registerAreaCode', 'xday'], values = ['reg','login', 'Recharge'])

# And_Cohort_Counrty_Data = And_Cohort_Counrty_Data.fillna(0)


## T1 -EN 
And_Cohort_Counrty_Data['Android数据', '1. T1-EN', '3 LTV'] = And_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','AU',1]+ i['Recharge','CA',1]+ i['Recharge','US',1]+ i['Recharge','GB',1]+
                    i['Recharge','AU',2]+ i['Recharge','CA',2]+ i['Recharge','US',2]+ i['Recharge','GB',2]+
                    i['Recharge','AU',3]+ i['Recharge','CA',3]+ i['Recharge','US',3]+ i['Recharge','GB',3])/( i['reg','AU',1]+ i['reg','CA',1]+ i['reg','US',1]+ i['reg','GB',1]) , axis = 1)

And_Cohort_Counrty_Data['Android数据', '1. T1-EN', '7 LTV'] = And_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','AU',1]+ i['Recharge','CA',1]+ i['Recharge','US',1]+ i['Recharge','GB',1]+
                    i['Recharge','AU',2]+ i['Recharge','CA',2]+ i['Recharge','US',2]+ i['Recharge','GB',2]+
                    i['Recharge','AU',3]+ i['Recharge','CA',3]+ i['Recharge','US',3]+ i['Recharge','GB',3]+
                    i['Recharge','AU',4]+ i['Recharge','CA',4]+ i['Recharge','US',4]+ i['Recharge','GB',4]+
                    i['Recharge','AU',5]+ i['Recharge','CA',5]+ i['Recharge','US',5]+ i['Recharge','GB',5]+
                    i['Recharge','AU',6]+ i['Recharge','CA',6]+ i['Recharge','US',6]+ i['Recharge','GB',6]+
                    i['Recharge','AU',7]+ i['Recharge','CA',7]+ i['Recharge','US',7]+ i['Recharge','GB',7] )/( i['reg','AU',1]+ i['reg','CA',1]+ i['reg','US',1]+ i['reg','GB',1]) , axis = 1)

And_Cohort_Counrty_Data['Android数据', '1. T1-EN', 'KPI: 7 LTV'] = ''

And_Cohort_Counrty_Data['Android数据', '1. T1-EN', '次留'] = And_Cohort_Counrty_Data.apply( 
        lambda i: ( i['login','AU',2]+ i['login','CA',2]+ i['login','US',2]+ i['login','GB',2])/( i['reg','AU',1]+ i['reg','CA',1]+ i['reg','US',1]+ i['reg','GB',1]) , axis = 1)

## DE 
And_Cohort_Counrty_Data['Android数据', '2. DE', '3 LTV'] = And_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','DE',1]+ i['Recharge','DE',2]+ i['Recharge','DE',3] )/( i['reg','DE',1]) , axis = 1)

And_Cohort_Counrty_Data['Android数据', '2. DE', '7 LTV'] = And_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','DE',1]+ i['Recharge','DE',2]+ i['Recharge','DE',3]+ i['Recharge','DE',4]+ 
                    i['Recharge','DE',5]+ i['Recharge','DE',6]+ i['Recharge','DE',7] )/( i['reg','DE',1]) , axis = 1)

And_Cohort_Counrty_Data['Android数据', '2. DE', 'KPI: 7 LTV'] = ''

And_Cohort_Counrty_Data['Android数据', '2. DE', '次留'] = And_Cohort_Counrty_Data.apply( 
        lambda i: ( i['login','DE',2])/( i['reg','DE',1]) , axis = 1)

## RU
And_Cohort_Counrty_Data['Android数据', '3. RU', '3 LTV'] = And_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','RU',1]+ i['Recharge','RU',2]+ i['Recharge','RU',3] )/( i['reg','RU',1]) , axis = 1)

And_Cohort_Counrty_Data['Android数据', '3. RU', '7 LTV'] = And_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','RU',1]+ i['Recharge','RU',2]+ i['Recharge','RU',3]+ i['Recharge','RU',4]+ 
                    i['Recharge','RU',5]+ i['Recharge','RU',6]+ i['Recharge','RU',7] )/( i['reg','RU',1]) , axis = 1)

And_Cohort_Counrty_Data['Android数据', '3. RU', 'KPI: 7 LTV'] = ''

And_Cohort_Counrty_Data['Android数据', '3. RU', '次留'] = And_Cohort_Counrty_Data.apply( 
        lambda i: ( i['login','RU',2])/( i['reg','RU',1]) , axis = 1)

## HK/TW
And_Cohort_Counrty_Data['Android数据', '4. HK/TW', '3 LTV'] = And_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','HK',1]+ i['Recharge','TW',1]+
                    i['Recharge','HK',2]+ i['Recharge','TW',2]+
                    i['Recharge','HK',3]+ i['Recharge','TW',3])/( i['reg','HK',1]+ i['reg','TW',1]) , axis = 1)

And_Cohort_Counrty_Data['Android数据', '4. HK/TW', '7 LTV'] = And_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','HK',1]+ i['Recharge','TW',1]+
                    i['Recharge','HK',2]+ i['Recharge','TW',2]+
                    i['Recharge','HK',3]+ i['Recharge','TW',3]+
                    i['Recharge','HK',4]+ i['Recharge','TW',4]+
                    i['Recharge','HK',5]+ i['Recharge','TW',5]+
                    i['Recharge','HK',6]+ i['Recharge','TW',6]+
                    i['Recharge','HK',7]+ i['Recharge','TW',7])/( i['reg','HK',1]+ i['reg','TW',1]) , axis = 1)

And_Cohort_Counrty_Data['Android数据', '4. HK/TW', 'KPI: 7 LTV'] = ''

And_Cohort_Counrty_Data['Android数据', '4. HK/TW', '次留'] = And_Cohort_Counrty_Data.apply( 
        lambda i: ( i['login','HK',2]+ i['login','TW',2])/( i['reg','HK',1]+ i['reg','TW',1]) , axis = 1)

## JP/KR
And_Cohort_Counrty_Data['Android数据', '5. JP/KR', '3 LTV'] = And_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','JP',1]+ i['Recharge','KR',1]+
                    i['Recharge','JP',2]+ i['Recharge','KR',2]+
                    i['Recharge','JP',3]+ i['Recharge','KR',3])/( i['reg','JP',1]+ i['reg','KR',1]) , axis = 1)

And_Cohort_Counrty_Data['Android数据', '5. JP/KR', '7 LTV'] = And_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','JP',1]+ i['Recharge','KR',1]+
                    i['Recharge','JP',2]+ i['Recharge','KR',2]+
                    i['Recharge','JP',3]+ i['Recharge','KR',3]+
                    i['Recharge','JP',4]+ i['Recharge','KR',4]+
                    i['Recharge','JP',5]+ i['Recharge','KR',5]+
                    i['Recharge','JP',6]+ i['Recharge','KR',6]+
                    i['Recharge','JP',7]+ i['Recharge','KR',7])/( i['reg','JP',1]+ i['reg','KR',1]) , axis = 1)

And_Cohort_Counrty_Data['Android数据', '5. JP/KR', 'KPI: 7 LTV'] = ''

And_Cohort_Counrty_Data['Android数据', '5. JP/KR', '次留'] = And_Cohort_Counrty_Data.apply( 
        lambda i: ( i['login','JP',2]+ i['login','KR',2])/( i['reg','JP',1]+ i['reg','KR',1]) , axis = 1)

And_Cohort_Counrty_Data = And_Cohort_Counrty_Data.ix[ :, And_Cohort_Counrty_Data.columns.get_level_values(2).isin({"3 LTV", "7 LTV", "KPI: 7 LTV", "次留"})]


## iOS 市场周数据
iOS_Cohort_Counrty = str(
    '''
        SELECT 
            a.Date, a.registerDate, a.xday, registerAreaCode,
            sum( a.loginCnt1) as login, sum( a.registerCnt1) as reg, 
            round( sum( a.paySum1)*0.49,2) as Recharge 
        FROM
        (
        	SELECT
        		Date, registerDate, DATEDIFF( Date, registerDate) +1 as xday, advertiser, countryGroup, registerAreaCode,
        		IFNULL( loginCnt,0) as loginCnt1, IFNULL( registerCnt,0) as registerCnt1, IFNULL( paySum,0) as paySum1
        	FROM
        		`t_data_track`
        	WHERE
        		registerDate BETWEEN '%s' AND '%s'
        	AND Date BETWEEN '%s' AND '%s'
        	AND advertiser != 'untrusted devices'  ## 假量
          AND platform = 'iOS'
        	AND gameCode = 'stgl'
          AND tier = 'T1'
        ) a
        GROUP BY a.Date, a.registerDate, a.xday, registerAreaCode
        HAVING a.xday <= 7;
''' %( date_calculate()[0], date_calculate()[1], date_calculate()[0], date_calculate()[1])) 

iOS_Cohort_Counrty_Data = pd.read_sql( iOS_Cohort_Counrty, con = connection)
iOS_Cohort_Counrty_Data = pd.merge( iOS_Cohort_Counrty_Data, Week_config, left_on = 'registerDate', right_on = 'Date')

iOS_Cohort_Counrty_Data = pd.pivot_table( iOS_Cohort_Counrty_Data, index = ['Class'], columns=['registerAreaCode', 'xday'], values = ['reg','login', 'Recharge'])

iOS_Cohort_Counrty_Data = iOS_Cohort_Counrty_Data.fillna(0)


## T1 -EN 
iOS_Cohort_Counrty_Data['iOS数据', '1. T1-EN', '3 LTV'] = iOS_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','AU',1]+ i['Recharge','CA',1]+ i['Recharge','US',1]+ i['Recharge','GB',1]+
                    i['Recharge','AU',2]+ i['Recharge','CA',2]+ i['Recharge','US',2]+ i['Recharge','GB',2]+
                    i['Recharge','AU',3]+ i['Recharge','CA',3]+ i['Recharge','US',3]+ i['Recharge','GB',3])/( i['reg','AU',1]+ i['reg','CA',1]+ i['reg','US',1]+ i['reg','GB',1]) , axis = 1)

iOS_Cohort_Counrty_Data['iOS数据', '1. T1-EN', '7 LTV'] = iOS_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','AU',1]+ i['Recharge','CA',1]+ i['Recharge','US',1]+ i['Recharge','GB',1]+
                    i['Recharge','AU',2]+ i['Recharge','CA',2]+ i['Recharge','US',2]+ i['Recharge','GB',2]+
                    i['Recharge','AU',3]+ i['Recharge','CA',3]+ i['Recharge','US',3]+ i['Recharge','GB',3]+
                    i['Recharge','AU',4]+ i['Recharge','CA',4]+ i['Recharge','US',4]+ i['Recharge','GB',4]+
                    i['Recharge','AU',5]+ i['Recharge','CA',5]+ i['Recharge','US',5]+ i['Recharge','GB',5]+
                    i['Recharge','AU',6]+ i['Recharge','CA',6]+ i['Recharge','US',6]+ i['Recharge','GB',6]+
                    i['Recharge','AU',7]+ i['Recharge','CA',7]+ i['Recharge','US',7]+ i['Recharge','GB',7] )/( i['reg','AU',1]+ i['reg','CA',1]+ i['reg','US',1]+ i['reg','GB',1]) , axis = 1)

iOS_Cohort_Counrty_Data['iOS数据', '1. T1-EN', 'KPI: 7 LTV'] = ''

iOS_Cohort_Counrty_Data['iOS数据', '1. T1-EN', '次留'] = iOS_Cohort_Counrty_Data.apply( 
        lambda i: ( i['login','AU',2]+ i['login','CA',2]+ i['login','US',2]+ i['login','GB',2])/( i['reg','AU',1]+ i['reg','CA',1]+ i['reg','US',1]+ i['reg','GB',1]) , axis = 1)

## DE 
iOS_Cohort_Counrty_Data['iOS数据', '2. DE', '3 LTV'] = iOS_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','DE',1]+ i['Recharge','DE',2]+ i['Recharge','DE',3] )/( i['reg','DE',1]) , axis = 1)

iOS_Cohort_Counrty_Data['iOS数据', '2. DE', '7 LTV'] = iOS_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','DE',1]+ i['Recharge','DE',2]+ i['Recharge','DE',3]+ i['Recharge','DE',4]+ 
                    i['Recharge','DE',5]+ i['Recharge','DE',6]+ i['Recharge','DE',7] )/( i['reg','DE',1]) , axis = 1)

iOS_Cohort_Counrty_Data['iOS数据', '2. DE', 'KPI: 7 LTV'] = ''

iOS_Cohort_Counrty_Data['iOS数据', '2. DE', '次留'] = iOS_Cohort_Counrty_Data.apply( 
        lambda i: ( i['login','DE',2])/( i['reg','DE',1]) , axis = 1)

## RU
iOS_Cohort_Counrty_Data['iOS数据', '3. RU', '3 LTV'] = iOS_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','RU',1]+ i['Recharge','RU',2]+ i['Recharge','RU',3] )/( i['reg','RU',1]) , axis = 1)

iOS_Cohort_Counrty_Data['iOS数据', '3. RU', '7 LTV'] = iOS_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','RU',1]+ i['Recharge','RU',2]+ i['Recharge','RU',3]+ i['Recharge','RU',4]+ 
                    i['Recharge','RU',5]+ i['Recharge','RU',6]+ i['Recharge','RU',7] )/( i['reg','RU',1]) , axis = 1)

iOS_Cohort_Counrty_Data['iOS数据', '3. RU', 'KPI: 7 LTV'] = ''

iOS_Cohort_Counrty_Data['iOS数据', '3. RU', '次留'] = iOS_Cohort_Counrty_Data.apply( 
        lambda i: ( i['login','RU',2])/( i['reg','RU',1]) , axis = 1)

## HK/TW
iOS_Cohort_Counrty_Data['iOS数据', '4. HK/TW', '3 LTV'] = iOS_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','HK',1]+ i['Recharge','TW',1]+
                    i['Recharge','HK',2]+ i['Recharge','TW',2]+
                    i['Recharge','HK',3]+ i['Recharge','TW',3])/( i['reg','HK',1]+ i['reg','TW',1]) , axis = 1)

iOS_Cohort_Counrty_Data['iOS数据', '4. HK/TW', '7 LTV'] = iOS_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','HK',1]+ i['Recharge','TW',1]+
                    i['Recharge','HK',2]+ i['Recharge','TW',2]+
                    i['Recharge','HK',3]+ i['Recharge','TW',3]+
                    i['Recharge','HK',4]+ i['Recharge','TW',4]+
                    i['Recharge','HK',5]+ i['Recharge','TW',5]+
                    i['Recharge','HK',6]+ i['Recharge','TW',6]+
                    i['Recharge','HK',7]+ i['Recharge','TW',7])/( i['reg','HK',1]+ i['reg','TW',1]) , axis = 1)

iOS_Cohort_Counrty_Data['iOS数据', '4. HK/TW', 'KPI: 7 LTV'] = ''

iOS_Cohort_Counrty_Data['iOS数据', '4. HK/TW', '次留'] = iOS_Cohort_Counrty_Data.apply( 
        lambda i: ( i['login','HK',2]+ i['login','TW',2])/( i['reg','JP',1]+ i['reg','KR',1]) , axis = 1)

## JP/KR
iOS_Cohort_Counrty_Data['iOS数据', '5. JP/KR', '3 LTV'] = iOS_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','JP',1]+ i['Recharge','KR',1]+
                    i['Recharge','JP',2]+ i['Recharge','KR',2]+
                    i['Recharge','JP',3]+ i['Recharge','KR',3])/( i['reg','JP',1]+ i['reg','KR',1]) , axis = 1)

iOS_Cohort_Counrty_Data['iOS数据', '5. JP/KR', '7 LTV'] = iOS_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','JP',1]+ i['Recharge','KR',1]+
                    i['Recharge','JP',2]+ i['Recharge','KR',2]+
                    i['Recharge','JP',3]+ i['Recharge','KR',3]+
                    i['Recharge','JP',4]+ i['Recharge','KR',4]+
                    i['Recharge','JP',5]+ i['Recharge','KR',5]+
                    i['Recharge','JP',6]+ i['Recharge','KR',6]+
                    i['Recharge','JP',7]+ i['Recharge','KR',7])/( i['reg','JP',1]+ i['reg','KR',1]) , axis = 1)

iOS_Cohort_Counrty_Data['iOS数据', '5. JP/KR', 'KPI: 7 LTV'] = ''

iOS_Cohort_Counrty_Data['iOS数据', '5. JP/KR', '次留'] = iOS_Cohort_Counrty_Data.apply( 
        lambda i: ( i['login','JP',2]+ i['login','KR',2])/( i['reg','JP',1]+ i['reg','KR',1]) , axis = 1)

iOS_Cohort_Counrty_Data = iOS_Cohort_Counrty_Data.ix[ :, iOS_Cohort_Counrty_Data.columns.get_level_values(2).isin({"3 LTV", "7 LTV", "KPI: 7 LTV", "次留"})]

## ------ 早会数据 ------
Meeting = pd.DataFrame( columns =  ['Date', 'Recharge', 'Pay_Rate', 'ARPPU',  'DNU', 'DAU', 'EDAU',
                                    '安卓次留', 'iOS次留', '1自然量LTV', '3LTV',  '7LTV', 'CPU', 'COST',
                                    '3ROI', '7ROI', 'And_Cost', 'iOS_Cost',  'Pay_Num'])
## 运营数据
Meeting['Date'] = And_actual_data[['Date']]
Meeting['Recharge'] = And_actual_data['Recharge'] + iOS_actual_data['Recharge']
Meeting['DNU'] = And_actual_data['DNU'] + iOS_actual_data['DNU']
Meeting['DAU'] = And_actual_data['DAU'] + iOS_actual_data['DAU']
Meeting['Pay_Num'] = And_actual_data['Pay_Num'] + iOS_actual_data['Pay_Num']
## 留存
Meeting['安卓次留'] = pd.DataFrame( list( And_Cohort_Data['留存', '次留']))
Meeting['iOS次留'] = pd.DataFrame( list( iOS_Cohort_Data['留存', '次留']))
## LTV
Meeting['1自然量LTV'] = pd.DataFrame( 
        list( 
                Whole_Cohoe_Data.apply( lambda i: (i['Recharge', '自然量', 1])/i['reg', '自然量', 1] , axis = 1)
                )
        )
Meeting['3LTV'] = pd.DataFrame( list( Whole_Cohoe_Data['3LTV', '整体', '3 LTV1']))
Meeting['7LTV'] = pd.DataFrame( list( Whole_Cohoe_Data['7LTV', '整体', '7 LTV1']))


## ------ Output ------
## Operation
output = str( r"C:\Users\Efun\Desktop\日报基础数据（产品） - %s.xlsx" % (date_calculate()[1]) )
writer = pd.ExcelWriter( output)

And_actual_data.to_excel( writer, index = False, encoding = 'utf-8', sheet_name = 'And_actual')
iOS_actual_data.to_excel( writer, index = False, encoding = 'utf-8', sheet_name = 'iOS_actual')
And.to_excel( writer, encoding = 'utf-8', sheet_name = 'And_dnu')
iOS.to_excel( writer, encoding = 'utf-8', sheet_name = 'iOS_dnu')
And_Cohort_Data_Op.to_excel( writer, encoding = 'utf-8', sheet_name = 'And_Cohort')
iOS_Cohort_Data_Op.to_excel( writer, encoding = 'utf-8', sheet_name = 'iOS_Cohort')
Whole_Cohoe_Data1.to_excel( writer, encoding = 'utf-8', sheet_name = '整体留存')
Whole_Cohoe_Data2.to_excel( writer, encoding = 'utf-8', sheet_name = '整体LTV')
Meeting.to_excel( writer, index = False, encoding = 'utf-8', sheet_name = '早会数据')

writer.save()

## Market
output = str( r"C:\Users\Efun\Desktop\日报基础数据（市场） - %s.xlsx" % (date_calculate()[1]) )
writer = pd.ExcelWriter( output)

And_Cohort_Data_Market.to_excel( writer, encoding = 'utf-8', sheet_name = '日报-Android')
iOS_Cohort_Data_Market.to_excel( writer, encoding = 'utf-8', sheet_name = '日报-iOS')
And_Market.to_excel( writer, encoding = 'utf-8', sheet_name = 'DNU监控-Android')
iOS_Market.to_excel( writer, encoding = 'utf-8', sheet_name = 'DNU监控-iOS')
And_Cohort_Counrty_Data.to_excel( writer, encoding = 'utf-8', sheet_name = 'LTV监控-分地区-Android')
iOS_Cohort_Counrty_Data.to_excel( writer, encoding = 'utf-8', sheet_name = 'LTV监控-分地区-iOS')

writer.save()



       "" as Cost, 
       SUM( registerCnt) as DNU,
       "" as CPU, 
      	SUM( loginCnt) as DAU,
       "" as EDAU, 
    	SUM( payCnt) as Pay_Num 
    FROM
    	`t_data_track`
    WHERE
    	Date BETWEEN '%s' AND '%s'
    AND advertiser != 'untrusted devices'   ## 去除假量
    AND platform = 'android'
    AND gameCode = 'stgl'
    GROUP BY Date
    ORDER BY Date;
''' %( date_calculate()[0], date_calculate()[1])) 
    

And_actual_data = pd.read_sql( And_actual_str, con = connection)
And_actual_data.loc[ :, "Week"] = And_actual_data.apply( 
            lambda i: weeknum( date = str( i[ "Date"])), axis = 1)

And_actual_data[ "Week"] = And_actual_data.apply( lambda i: weeknum( str( i['Date'])) , axis = 1)

## iOS Actual Data
iOS_actual_str = str(
    '''
    SELECT
    	Date,
       "" as Week,
    	ROUND( SUM( paySum),2) as Recharge,
       "" as Cost, 
       SUM( registerCnt) as DNU,
       "" as CPU, 
      	SUM( loginCnt) as DAU,
       "" as EDAU, 
    	SUM( payCnt) as Pay_Num 
    FROM
    	`t_data_track`
    WHERE
    	Date BETWEEN '%s' AND '%s'
    AND advertiser != 'untrusted devices'   ## 去除假量
    AND platform = 'ios'
    AND gameCode = 'stgl'
    GROUP BY Date
    ORDER BY Date;
''' %( date_calculate()[0], date_calculate()[1])) 
    

iOS_actual_data = pd.read_sql( iOS_actual_str, con = connection)
iOS_actual_data.loc[ :, "Week"] = iOS_actual_data.apply( 
            lambda i: weeknum( date = str( i[ "Date"])), axis = 1)


## ------ DNU 占比 ------
Index = pd.DataFrame({
        'Index1': ["T1", "ROW", "AU", "CA", "US", "GB", "DE", "FR", "RU", "TW", "HK", "JP", "KR"],
        'Index': ["01.T1", "02.ROW", "03.AU", "04.CA", "05.US", "06.GB", "07.DE", "08.FR", "09.RU", "10.TW", "11.HK", "12.JP", "13.KR"]
        })
## "CH", "AT", "CN", "MO" 剔除, 方面下面做 Merge时直接剔除这四个地区
    
And_Country_Dnu_str = str(
    '''
        SELECT
        	Date,
        	registerAreaCode as Index1,
        	SUM( IFNULL( registerCnt,0)) AS DNU
        FROM
        	`t_data_track`
        WHERE
        	Date BETWEEN '%s' AND '%s'
        AND advertiser != 'untrusted devices' ## 去除假量
        AND platform = 'android'
        AND gameCode = 'stgl'
        AND tier = 'T1'
        GROUP BY
        	Date, Index1, tier
        ORDER BY
        	Date;
''' %( date_calculate()[0], date_calculate()[1])) 
    
And_Tier_Dnu_str = str(
    '''
        SELECT
        	Date,
        	CASE 
        		WHEN tier = 'T1' THEN 'T1'
        		WHEN tier <> 'T1' THEN 'ROW'
        	END AS Index1,
        	SUM( IFNULL( registerCnt,0)) AS DNU
        FROM
        	`t_data_track`
        WHERE
        	Date BETWEEN '%s' AND '%s'
        AND advertiser != 'untrusted devices' ## 去除假量
        AND platform = 'android'
        AND gameCode = 'stgl'
        GROUP BY
        	Date, Index1
        ORDER BY
        	Date;
''' %( date_calculate()[0], date_calculate()[1])) 
    

And_Country_Dnu_data = pd.read_sql( And_Country_Dnu_str, con = connection)
And_Tier_Dnu_str_data = pd.read_sql( And_Tier_Dnu_str, con = connection)
And_Country_Dnu_data = pd.concat( [ And_Country_Dnu_data, And_Tier_Dnu_str_data])
And_Country_Dnu_data = pd.merge( And_Country_Dnu_data, Index, on = 'Index1')

And = pd.pivot_table( And_Country_Dnu_data, index = ['Date'], columns=['Index'], values = ['DNU'])
And = And.fillna(0)
And['DNU', '00.DNU'] = And.apply( lambda i: i['DNU', "01.T1"] + i['DNU', "02.ROW"] , axis = 1)
And = And.sort_index( axis = 1)
# And.ix[ :, And.columns.get_level_values(1).isin({"00.DNU", "01.T1", "02.ROW", "03.AU", "04.CA", "05.US", "06.GB", "07.DE", "08.FR", "09.RU", "10.TW", "11.HK", "12.JP", "13.KR"})]

And_Market = copy.deepcopy( And)
And_Market['DNU', '1.T1-EN'] = And_Market.apply( lambda i:  i['DNU', "03.AU"] + i['DNU', "04.CA"] + i['DNU', "05.US"] + i['DNU', "06.GB"] , axis = 1)
And_Market['DNU', '2.DE'] = And_Market.apply( lambda i:  i['DNU', "07.DE"] , axis = 1)
And_Market['DNU', '3.RU'] = And_Market.apply( lambda i:  i['DNU', "09.RU"] , axis = 1)
And_Market['DNU', '4.HK/TW/JP/KR'] = And_Market.apply( lambda i:  i['DNU', "10.TW"] + i['DNU', "11.HK"] + i['DNU', "12.JP"] + i['DNU', "13.KR"] , axis = 1)
And_Market['DNU', '5.ROW'] = And_Market.apply( lambda i:  i['DNU', "02.ROW"] , axis = 1)

And_Market = And_Market.ix[ :, And_Market.columns.get_level_values(1).isin({"1.T1-EN", "2.DE", "3.RU", "4.HK/TW/JP/KR", "5.ROW"})]


iOS_Country_Dnu_str = str(
    '''
        SELECT
        	Date,
        	registerAreaCode as Index1,
        	SUM( IFNULL( registerCnt,0)) AS DNU
        FROM
        	`t_data_track`
        WHERE
        	Date BETWEEN '%s' AND '%s'
        AND advertiser != 'untrusted devices' ## 去除假量
        AND platform = 'ios'
        AND gameCode = 'stgl'
        AND tier = 'T1'
        GROUP BY
        	Date, Index1, tier
        ORDER BY
        	Date;
''' %( date_calculate()[0], date_calculate()[1])) 
    
iOS_Tier_Dnu_str = str(
    '''
        SELECT
        	Date,
        	CASE 
        		WHEN tier = 'T1' THEN 'T1'
        		WHEN tier <> 'T1' THEN 'ROW'
        	END AS Index1,
        	SUM( IFNULL( registerCnt,0)) AS DNU
        FROM
        	`t_data_track`
        WHERE
        	Date BETWEEN '%s' AND '%s'
        AND advertiser != 'untrusted devices' ## 去除假量
        AND platform = 'iOS'
        AND gameCode = 'stgl'
        GROUP BY
        	Date, Index1
        ORDER BY
        	Date;
''' %( date_calculate()[0], date_calculate()[1])) 
    

iOS_Country_Dnu_data = pd.read_sql( iOS_Country_Dnu_str, con = connection)
iOS_Tier_Dnu_str_data = pd.read_sql( iOS_Tier_Dnu_str, con = connection)
iOS_Country_Dnu_data = pd.concat( [ iOS_Country_Dnu_data, iOS_Tier_Dnu_str_data])
iOS_Country_Dnu_data = pd.merge( iOS_Country_Dnu_data, Index, on = 'Index1')

iOS = pd.pivot_table( iOS_Country_Dnu_data, index = ['Date'], columns=['Index'], values = ['DNU'])
iOS = iOS.fillna(0)
iOS['DNU', '00.DNU'] = iOS.apply( lambda i: i['DNU', "01.T1"] + i['DNU', "02.ROW"] , axis = 1)
iOS = iOS.sort_index( axis = 1)
#iOS.ix[ :, iOS.columns.get_level_values(1).isin({"01.T1", "02.ROW", "03.AU", "04.CA", "05.US", "06.GB", "07.DE", "08.FR", "09.RU", "10.TW", "11.HK", "12.JP", "13.KR"})]

iOS_Market = copy.deepcopy( iOS)
iOS_Market['DNU', '1.T1-EN'] = iOS_Market.apply( lambda i:  i['DNU', "03.AU"] + i['DNU', "04.CA"] + i['DNU', "05.US"] + i['DNU', "06.GB"] , axis = 1)
iOS_Market['DNU', '2.DE'] = iOS_Market.apply( lambda i:  i['DNU', "07.DE"] , axis = 1)
iOS_Market['DNU', '3.RU'] = iOS_Market.apply( lambda i:  i['DNU', "09.RU"] , axis = 1)
iOS_Market['DNU', '4.HK/TW/JP/KR'] = iOS_Market.apply( lambda i:  i['DNU', "10.TW"] + i['DNU', "11.HK"] + i['DNU', "12.JP"] + i['DNU', "13.KR"] , axis = 1)
iOS_Market['DNU', '5.ROW'] = iOS_Market.apply( lambda i:  i['DNU', "02.ROW"] , axis = 1)

iOS_Market = iOS_Market.ix[ :, iOS_Market.columns.get_level_values(1).isin({"1.T1-EN", "2.DE", "3.RU", "4.HK/TW/JP/KR", "5.ROW"})]


## ------ 留存Cohort ------
And_Cohort = str(
    '''
        SELECT 
            a.Date, a.registerDate, a.xday, 
            sum( a.loginCnt1) as login, sum( a.registerCnt1) as reg, 
            round( sum( a.paySum1)*0.49,2) as Recharge 
        FROM
        (
        	SELECT
        		Date, registerDate, DATEDIFF( Date, registerDate) +1 as xday, advertiser, countryGroup, 
        		IFNULL( loginCnt,0) as loginCnt1, IFNULL( registerCnt,0) as registerCnt1, IFNULL( paySum,0) as paySum1
        	FROM
        		`t_data_track`
        	WHERE
        		registerDate BETWEEN '%s' AND '%s'
        	AND Date BETWEEN '%s' AND '%s'
        	AND advertiser != 'untrusted devices'  ## 假量
          AND platform = 'android'
        	AND gameCode = 'stgl'
        ) a
        GROUP BY a.Date, a.registerDate, a.xday
        HAVING a.xday <= 7;
''' %( date_calculate()[0], date_calculate()[1], date_calculate()[0], date_calculate()[1])) 

And_Cohort_Data = pd.read_sql( And_Cohort, con = connection)
And_Cohort_Data = pd.pivot_table( And_Cohort_Data, index = ['registerDate'], columns=['xday'], values = ['reg','login', 'Recharge'])
And_Cohort_Data = And_Cohort_Data.fillna(0)

And_Cohort_Data['留存', 'DNU'] = And_Cohort_Data['reg', 1]
And_Cohort_Data['留存', '次留'] = And_Cohort_Data.apply( lambda i: i['login', 2]/i['reg', 1] , axis = 1)
And_Cohort_Data['留存', '7留'] = And_Cohort_Data.apply( lambda i: i['login', 7]/i['reg', 1] , axis = 1)
And_Cohort_Data['留存', 'CPU'] = ''

And_Cohort_Data['LTV', '1 LTV'] = And_Cohort_Data.apply( 
        lambda i: i['Recharge', 1]/i['reg', 1] , axis = 1)
And_Cohort_Data['LTV', '1 ROI'] = ''
And_Cohort_Data['LTV', '3 LTV'] = And_Cohort_Data.apply( 
        lambda i: '-' if i['Recharge', 3] == 0 else (i['Recharge', 1]+i['Recharge', 2]+i['Recharge', 3])/i['reg', 1] , axis = 1)
And_Cohort_Data['LTV', '3 ROI'] = ''
And_Cohort_Data['LTV', '7 LTV'] = And_Cohort_Data.apply( 
        lambda i: '-' if i['Recharge', 7] == 0 else (i['Recharge', 1]+i['Recharge', 2]+i['Recharge', 3]+i['Recharge', 4]+i['Recharge', 5]+i['Recharge', 6]+i['Recharge', 7])/i['reg', 1] , axis = 1)


And_Cohort_Data_Op = And_Cohort_Data.ix[ :, And_Cohort_Data.columns.get_level_values(1).isin({"DNU", "次留", "7留", "CPU", "3 LTV", "3 ROI", "7 LTV"})]
## 市场日报
And_Cohort_Data_Market = And_Cohort_Data.ix[ :, And_Cohort_Data.columns.get_level_values(1).isin({"DNU", "次留", "1 LTV", "1 ROI", "3 LTV", "3 ROI", "7 LTV"})]  


iOS_Cohort = str(
    '''
        SELECT 
            a.Date, a.registerDate, a.xday, 
            sum( a.loginCnt1) as login, sum( a.registerCnt1) as reg, 
            round( sum( a.paySum1)*0.49,2) as Recharge 
        FROM
        (
        	SELECT
        		Date, registerDate, DATEDIFF( Date, registerDate) +1 as xday, advertiser, countryGroup, 
        		IFNULL( loginCnt,0) as loginCnt1, IFNULL( registerCnt,0) as registerCnt1, IFNULL( paySum,0) as paySum1
        	FROM
        		`t_data_track`
        	WHERE
        		registerDate BETWEEN '%s' AND '%s'
        	AND Date BETWEEN '%s' AND '%s'
        	AND advertiser != 'untrusted devices'  ## 假量
          AND platform = 'ios'
        	AND gameCode = 'stgl'
        ) a
        GROUP BY a.Date, a.registerDate, a.xday
        HAVING a.xday <= 7;
''' %( date_calculate()[0], date_calculate()[1], date_calculate()[0], date_calculate()[1])) 

iOS_Cohort_Data = pd.read_sql( iOS_Cohort, con = connection)
iOS_Cohort_Data = pd.pivot_table( iOS_Cohort_Data, index = ['registerDate'], columns=['xday'], values = ['reg','login', 'Recharge'])
iOS_Cohort_Data = iOS_Cohort_Data.fillna(0)

iOS_Cohort_Data['留存', 'DNU'] = iOS_Cohort_Data['reg', 1]
iOS_Cohort_Data['留存', '次留'] = iOS_Cohort_Data.apply( lambda i: i['login', 2]/i['reg', 1] , axis = 1)
iOS_Cohort_Data['留存', '7留'] = iOS_Cohort_Data.apply( lambda i: i['login', 7]/i['reg', 1] , axis = 1)
iOS_Cohort_Data['留存', 'CPU'] = ''

iOS_Cohort_Data['LTV', '1 LTV'] = iOS_Cohort_Data.apply( 
        lambda i: i['Recharge', 1]/i['reg', 1] , axis = 1)
iOS_Cohort_Data['LTV', '1 ROI'] = ''
iOS_Cohort_Data['LTV', '3 LTV'] = iOS_Cohort_Data.apply( 
        lambda i: '-' if i['Recharge', 3] == 0 else (i['Recharge', 1]+i['Recharge', 2]+i['Recharge', 3])/i['reg', 1] , axis = 1)
iOS_Cohort_Data['LTV', '3 ROI'] = ''
iOS_Cohort_Data['LTV', '7 LTV'] = iOS_Cohort_Data.apply( 
        lambda i: '-' if i['Recharge', 7] == 0 else (i['Recharge', 1]+i['Recharge', 2]+i['Recharge', 3]+i['Recharge', 4]+i['Recharge', 5]+i['Recharge', 6]+i['Recharge', 7])/i['reg', 1] , axis = 1)

iOS_Cohort_Data_Op = iOS_Cohort_Data.ix[ :, iOS_Cohort_Data.columns.get_level_values(1).isin({"DNU", "次留", "7留", "CPU", "3 LTV", "3 ROI", "7 LTV"})]
## 市场日报
iOS_Cohort_Data_Market = iOS_Cohort_Data.ix[ :, iOS_Cohort_Data.columns.get_level_values(1).isin({"DNU", "次留", "1 LTV", "1 ROI", "3 LTV", "3 ROI", "7 LTV"})]  


Whole_Cohor = str(
    '''
        SELECT 
            a.Date, "整体" as Type, a.registerDate, a.xday, 
            sum( a.loginCnt1) as login, sum( a.registerCnt1) as reg, round( sum( a.paySum1)*0.49,2) as Recharge 
        FROM
        (
        	SELECT
        		Date, registerDate, DATEDIFF( Date, registerDate) +1 as xday, advertiser, countryGroup, 
        		IFNULL( loginCnt,0) as loginCnt1, IFNULL( registerCnt,0) as registerCnt1, IFNULL( paySum,0) as paySum1
        	FROM
        		`t_data_track`
        	WHERE
        		registerDate BETWEEN '%s' AND '%s'
        	AND Date BETWEEN '%s' AND '%s'
        	AND advertiser != 'untrusted devices'  ## 假量
        	AND gameCode = 'stgl'
        ) a
        WHERE a.xday <= 7
        GROUP BY a.Date, a.registerDate, a.xday
        UNION
        SELECT 
            a.Date, "自然量" as Type, a.registerDate, a.xday, 
            sum( a.loginCnt1) as login, sum( a.registerCnt1) as reg, round( sum( a.paySum1)*0.49,2) as Recharge 
        FROM
        (
        	SELECT
        		Date, registerDate, DATEDIFF( Date, registerDate) +1 as xday, advertiser, countryGroup, 
        		IFNULL( loginCnt,0) as loginCnt1, IFNULL( registerCnt,0) as registerCnt1, IFNULL( paySum,0) as paySum1
        	FROM
        		`t_data_track`
        	WHERE
        		registerDate BETWEEN '%s' AND '%s'
        	AND Date BETWEEN '%s' AND '%s'
        	AND advertiser != 'untrusted devices'  ## 假量
        	AND advertiser = 'organic'
        	AND gameCode = 'stgl'
        ) a
        WHERE a.xday <= 7
        GROUP BY a.Date, a.registerDate, a.xday
        UNION
        SELECT 
            a.Date, "渠道量" as Type, a.registerDate, a.xday, 
            sum( a.loginCnt1) as login, sum( a.registerCnt1) as reg, round( sum( a.paySum1)*0.49,2) as Recharge  
        FROM
        (
        	SELECT
        		Date, registerDate, DATEDIFF( Date, registerDate) +1 as xday, advertiser, countryGroup, 
        		IFNULL( loginCnt,0) as loginCnt1, IFNULL( registerCnt,0) as registerCnt1, IFNULL( paySum,0) as paySum1
        	FROM
        		`t_data_track`
        	WHERE
        		registerDate BETWEEN '%s' AND '%s'
        	AND Date BETWEEN '%s' AND '%s'
        	AND advertiser != 'untrusted devices'  ## 假量
        	AND advertiser != 'organic'
        	AND gameCode = 'stgl'
        ) a
        WHERE a.xday <= 7
        GROUP BY a.Date, a.registerDate, a.xday;
''' %( date_calculate()[0], date_calculate()[1], date_calculate()[0], date_calculate()[1],
        date_calculate()[0], date_calculate()[1], date_calculate()[0], date_calculate()[1],
        date_calculate()[0], date_calculate()[1], date_calculate()[0], date_calculate()[1])) 

Whole_Cohoe_Data = pd.read_sql( Whole_Cohor, con = connection)
Whole_Cohoe_Data = pd.pivot_table( Whole_Cohoe_Data, index = ['registerDate'], columns=['Type','xday'], values = ['reg','login', 'Recharge'])
Whole_Cohoe_Data = Whole_Cohoe_Data.fillna(0)

## ------ 整体留存 ------
Whole_Cohoe_Data['留存', '整体', 'DNU'] = Whole_Cohoe_Data['reg', '整体', 1]
Whole_Cohoe_Data['次日留存率', '整体', '次留1'] = Whole_Cohoe_Data.apply( lambda i: i['login', '整体', 2]/i['reg', '整体', 1] , axis = 1)
Whole_Cohoe_Data['次日留存率', '自然量', '次留2'] = Whole_Cohoe_Data.apply( lambda i: i['login', '自然量', 2]/i['reg', '自然量', 1] , axis = 1)
Whole_Cohoe_Data['次日留存率', '渠道量', '次留3'] = Whole_Cohoe_Data.apply( lambda i: i['login', '渠道量', 2]/i['reg', '渠道量', 1] , axis = 1)
Whole_Cohoe_Data['次日留存率', '整体', '7留1'] = Whole_Cohoe_Data.apply( lambda i: i['login', '整体', 7]/i['reg', '整体', 1] , axis = 1)
Whole_Cohoe_Data['次日留存率', '自然量', '7留2'] = Whole_Cohoe_Data.apply( lambda i: i['login', '自然量', 7]/i['reg', '自然量', 1] , axis = 1)
Whole_Cohoe_Data['次日留存率', '渠道量', '7留3'] = Whole_Cohoe_Data.apply( lambda i: i['login', '渠道量', 7]/i['reg', '渠道量', 1] , axis = 1)

Whole_Cohoe_Data1 = Whole_Cohoe_Data.ix[ :, Whole_Cohoe_Data.columns.get_level_values(2).isin({"DNU", "次留1", "次留2", "次留3", "7留1", "7留2", "7留3"})]


## ------ 整体LTV ------
Whole_Cohoe_Data['LTV', '整体', '1 ROI'] = ''
Whole_Cohoe_Data['LTV', '整体', '3 ROI'] = ''
Whole_Cohoe_Data['LTV', '整体', '7 ROI'] = ''
Whole_Cohoe_Data['LTV', '整体', '7ROI/3ROI'] = ''

Whole_Cohoe_Data['LTV', '整体', '1 LTV'] = Whole_Cohoe_Data.apply( 
        lambda i: (i['Recharge', '整体', 1])/i['reg', '整体', 1] , axis = 1)
Whole_Cohoe_Data['3LTV', '整体', '3 LTV1'] = Whole_Cohoe_Data.apply( 
        lambda i: '-' if i['Recharge', '整体', 3] == 0 else (i['Recharge', '整体', 1]+i['Recharge', '整体', 2]+i['Recharge', '整体', 3])/i['reg', '整体', 1] , axis = 1)
Whole_Cohoe_Data['3LTV', '自然量', '3 LTV2'] = Whole_Cohoe_Data.apply( 
        lambda i: '-' if i['Recharge', '自然量', 3] == 0 else (i['Recharge', '自然量', 1]+i['Recharge', '自然量', 2]+i['Recharge', '自然量', 3])/i['reg', '自然量', 1] , axis = 1)
Whole_Cohoe_Data['3LTV', '渠道量', '3 LTV3'] = Whole_Cohoe_Data.apply( 
        lambda i: '-' if i['Recharge', '渠道量', 3] == 0 else (i['Recharge', '渠道量', 1]+i['Recharge', '渠道量', 2]+i['Recharge', '渠道量', 3])/i['reg', '渠道量', 1] , axis = 1)

Whole_Cohoe_Data['7LTV', '整体', '7 LTV1'] = Whole_Cohoe_Data.apply( 
        lambda i: '-' if i['Recharge', '整体', 7] == 0 else (i['Recharge', '整体', 1]+i['Recharge', '整体', 2]+i['Recharge', '整体', 3]+i['Recharge', '整体', 4]+i['Recharge', '整体', 5]+i['Recharge', '整体', 6]+i['Recharge', '整体', 7])/i['reg', '整体', 1] , axis = 1)
Whole_Cohoe_Data['7LTV', '自然量', '7 LTV2'] = Whole_Cohoe_Data.apply( 
        lambda i: '-' if i['Recharge', '自然量', 7] == 0 else (i['Recharge', '自然量', 1]+i['Recharge', '自然量', 2]+i['Recharge', '自然量', 3]+i['Recharge', '自然量', 4]+i['Recharge', '自然量', 5]+i['Recharge', '自然量', 6]+i['Recharge', '自然量', 7])/i['reg', '自然量', 1] , axis = 1)
Whole_Cohoe_Data['7LTV', '渠道量', '7 LTV3'] = Whole_Cohoe_Data.apply( 
        lambda i: '-' if i['Recharge', '渠道量', 7] == 0 else (i['Recharge', '渠道量', 1]+i['Recharge', '渠道量', 2]+i['Recharge', '渠道量', 3]+i['Recharge', '渠道量', 4]+i['Recharge', '渠道量', 5]+i['Recharge', '渠道量', 6]+i['Recharge', '渠道量', 7])/i['reg', '渠道量', 1] , axis = 1)

Whole_Cohoe_Data2 = Whole_Cohoe_Data.ix[ :, Whole_Cohoe_Data.columns.get_level_values(2).isin({"DNU", "1 ROI", "3 ROI", "7 ROI", "7ROI/3ROI", "1 LTV", "3 LTV1", "3 LTV2", "3 LTV3", "7 LTV1", "7 LTV2", "7 LTV3"})]


## ------ DNU分生命周期 ------
## 登录表查询太慢了, 暂时不放在这个脚本中；


## ------ Marketing ------
def make_config():
    Week_config = copy.deepcopy( And_actual_data.loc[ :,"Date": "Week"])
    Week_config['Temp'] = Week_config.apply( 
            lambda i: '3天' if i['Week'] >= 3 and i['Week'] <= 5 else '4天', axis = 1)
    
    Week_config['Temp1'] = 1 
    for i in range( 1, len( Week_config)):
        if Week_config.loc[ i, 'Temp'] == Week_config.loc[ i-1, 'Temp']:
            Week_config.loc[ i, 'Temp1'] = Week_config.loc[ i-1, 'Temp1'] 
        else:
            Week_config.loc[ i, 'Temp1'] = Week_config.loc[ i-1, 'Temp1'] + 1
            
    Week_config1 = pd.DataFrame({ 'Temp1': list( range( min( Week_config['Temp1']), max( Week_config['Temp1']+1) ) ),
                                'Class' : '',
                                'Day_num' : ''})
    
    for i in Week_config1['Temp1']:
        Week_config1.loc[ i-1, 'Class'] = str( 
                str( i) + ". " +
                min( Week_config[ Week_config['Temp1'] == i]['Date']).strftime( '%m.%d') + 
                "-" + 
                max( Week_config[ Week_config['Temp1'] == i]['Date']).strftime( '%m.%d')
                )
        Week_config1.loc[ i-1, 'Day_num'] = len( Week_config[ Week_config['Temp1'] == i])
        
    Week_config = pd.merge( Week_config, Week_config1, on = 'Temp1')
    del Week_config1
    return( Week_config.loc[ :,['Date', 'Temp', 'Class', 'Day_num']])

Week_config = make_config()


## Android 市场周数据
And_Cohort_Counrty = str(
    '''
        SELECT 
            a.Date, a.registerDate, a.xday, registerAreaCode,
            sum( a.loginCnt1) as login, sum( a.registerCnt1) as reg, 
            round( sum( a.paySum1)*0.49,2) as Recharge 
        FROM
        (
        	SELECT
        		Date, registerDate, DATEDIFF( Date, registerDate) +1 as xday, advertiser, countryGroup, registerAreaCode,
        		IFNULL( loginCnt,0) as loginCnt1, IFNULL( registerCnt,0) as registerCnt1, IFNULL( paySum,0) as paySum1
        	FROM
        		`t_data_track`
        	WHERE
        		registerDate BETWEEN '%s' AND '%s'
        	AND Date BETWEEN '%s' AND '%s'
        	AND advertiser != 'untrusted devices'  ## 假量
          AND platform = 'android'
        	AND gameCode = 'stgl'
          AND tier = 'T1'
        ) a
        GROUP BY a.Date, a.registerDate, a.xday, registerAreaCode
        HAVING a.xday <= 7;
''' %( date_calculate()[0], date_calculate()[1], date_calculate()[0], date_calculate()[1])) 

And_Cohort_Counrty_Data = pd.read_sql( And_Cohort_Counrty, con = connection)
And_Cohort_Counrty_Data = pd.merge( And_Cohort_Counrty_Data, Week_config, left_on = 'registerDate', right_on = 'Date')

And_Cohort_Counrty_Data = pd.pivot_table( And_Cohort_Counrty_Data, index = ['Class'], columns=['registerAreaCode', 'xday'], values = ['reg','login', 'Recharge'])

And_Cohort_Counrty_Data = And_Cohort_Counrty_Data.fillna(0)


## T1 -EN 
And_Cohort_Counrty_Data['Android数据', '1. T1-EN', '3 LTV'] = And_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','AU',1]+ i['Recharge','CA',1]+ i['Recharge','US',1]+ i['Recharge','GB',1]+
                    i['Recharge','AU',2]+ i['Recharge','CA',2]+ i['Recharge','US',2]+ i['Recharge','GB',2]+
                    i['Recharge','AU',3]+ i['Recharge','CA',3]+ i['Recharge','US',3]+ i['Recharge','GB',3])/( i['reg','AU',1]+ i['reg','CA',1]+ i['reg','US',1]+ i['reg','GB',1]) , axis = 1)

And_Cohort_Counrty_Data['Android数据', '1. T1-EN', '7 LTV'] = And_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','AU',1]+ i['Recharge','CA',1]+ i['Recharge','US',1]+ i['Recharge','GB',1]+
                    i['Recharge','AU',2]+ i['Recharge','CA',2]+ i['Recharge','US',2]+ i['Recharge','GB',2]+
                    i['Recharge','AU',3]+ i['Recharge','CA',3]+ i['Recharge','US',3]+ i['Recharge','GB',3]+
                    i['Recharge','AU',4]+ i['Recharge','CA',4]+ i['Recharge','US',4]+ i['Recharge','GB',4]+
                    i['Recharge','AU',5]+ i['Recharge','CA',5]+ i['Recharge','US',5]+ i['Recharge','GB',5]+
                    i['Recharge','AU',6]+ i['Recharge','CA',6]+ i['Recharge','US',6]+ i['Recharge','GB',6]+
                    i['Recharge','AU',7]+ i['Recharge','CA',7]+ i['Recharge','US',7]+ i['Recharge','GB',7] )/( i['reg','AU',1]+ i['reg','CA',1]+ i['reg','US',1]+ i['reg','GB',1]) , axis = 1)

And_Cohort_Counrty_Data['Android数据', '1. T1-EN', 'KPI: 7 LTV'] = ''

And_Cohort_Counrty_Data['Android数据', '1. T1-EN', '次留'] = And_Cohort_Counrty_Data.apply( 
        lambda i: ( i['login','AU',2]+ i['login','CA',2]+ i['login','US',2]+ i['login','GB',2])/( i['reg','AU',1]+ i['reg','CA',1]+ i['reg','US',1]+ i['reg','GB',1]) , axis = 1)

## DE 
And_Cohort_Counrty_Data['Android数据', '2. DE', '3 LTV'] = And_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','DE',1]+ i['Recharge','DE',2]+ i['Recharge','DE',3] )/( i['reg','DE',1]) , axis = 1)

And_Cohort_Counrty_Data['Android数据', '2. DE', '7 LTV'] = And_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','DE',1]+ i['Recharge','DE',2]+ i['Recharge','DE',3]+ i['Recharge','DE',4]+ 
                    i['Recharge','DE',5]+ i['Recharge','DE',6]+ i['Recharge','DE',7] )/( i['reg','DE',1]) , axis = 1)

And_Cohort_Counrty_Data['Android数据', '2. DE', 'KPI: 7 LTV'] = ''

And_Cohort_Counrty_Data['Android数据', '2. DE', '次留'] = And_Cohort_Counrty_Data.apply( 
        lambda i: ( i['login','DE',2])/( i['reg','DE',1]) , axis = 1)

## RU
And_Cohort_Counrty_Data['Android数据', '3. RU', '3 LTV'] = And_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','RU',1]+ i['Recharge','RU',2]+ i['Recharge','RU',3] )/( i['reg','RU',1]) , axis = 1)

And_Cohort_Counrty_Data['Android数据', '3. RU', '7 LTV'] = And_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','RU',1]+ i['Recharge','RU',2]+ i['Recharge','RU',3]+ i['Recharge','RU',4]+ 
                    i['Recharge','RU',5]+ i['Recharge','RU',6]+ i['Recharge','RU',7] )/( i['reg','RU',1]) , axis = 1)

And_Cohort_Counrty_Data['Android数据', '3. RU', 'KPI: 7 LTV'] = ''

And_Cohort_Counrty_Data['Android数据', '3. RU', '次留'] = And_Cohort_Counrty_Data.apply( 
        lambda i: ( i['login','RU',2])/( i['reg','RU',1]) , axis = 1)

## HK/TW
And_Cohort_Counrty_Data['Android数据', '4. HK/TW', '3 LTV'] = And_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','HK',1]+ i['Recharge','TW',1]+
                    i['Recharge','HK',2]+ i['Recharge','TW',2]+
                    i['Recharge','HK',3]+ i['Recharge','TW',3])/( i['reg','HK',1]+ i['reg','TW',1]) , axis = 1)

And_Cohort_Counrty_Data['Android数据', '4. HK/TW', '7 LTV'] = And_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','HK',1]+ i['Recharge','TW',1]+
                    i['Recharge','HK',2]+ i['Recharge','TW',2]+
                    i['Recharge','HK',3]+ i['Recharge','TW',3]+
                    i['Recharge','HK',4]+ i['Recharge','TW',4]+
                    i['Recharge','HK',5]+ i['Recharge','TW',5]+
                    i['Recharge','HK',6]+ i['Recharge','TW',6]+
                    i['Recharge','HK',7]+ i['Recharge','TW',7])/( i['reg','HK',1]+ i['reg','TW',1]) , axis = 1)

And_Cohort_Counrty_Data['Android数据', '4. HK/TW', 'KPI: 7 LTV'] = ''

And_Cohort_Counrty_Data['Android数据', '4. HK/TW', '次留'] = And_Cohort_Counrty_Data.apply( 
        lambda i: ( i['login','HK',2]+ i['login','TW',2])/( i['reg','HK',1]+ i['reg','TW',1]) , axis = 1)

## JP/KR
And_Cohort_Counrty_Data['Android数据', '5. JP/KR', '3 LTV'] = And_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','JP',1]+ i['Recharge','KR',1]+
                    i['Recharge','JP',2]+ i['Recharge','KR',2]+
                    i['Recharge','JP',3]+ i['Recharge','KR',3])/( i['reg','JP',1]+ i['reg','KR',1]) , axis = 1)

And_Cohort_Counrty_Data['Android数据', '5. JP/KR', '7 LTV'] = And_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','JP',1]+ i['Recharge','KR',1]+
                    i['Recharge','JP',2]+ i['Recharge','KR',2]+
                    i['Recharge','JP',3]+ i['Recharge','KR',3]+
                    i['Recharge','JP',4]+ i['Recharge','KR',4]+
                    i['Recharge','JP',5]+ i['Recharge','KR',5]+
                    i['Recharge','JP',6]+ i['Recharge','KR',6]+
                    i['Recharge','JP',7]+ i['Recharge','KR',7])/( i['reg','JP',1]+ i['reg','KR',1]) , axis = 1)

And_Cohort_Counrty_Data['Android数据', '5. JP/KR', 'KPI: 7 LTV'] = ''

And_Cohort_Counrty_Data['Android数据', '5. JP/KR', '次留'] = And_Cohort_Counrty_Data.apply( 
        lambda i: ( i['login','JP',2]+ i['login','KR',2])/( i['reg','JP',1]+ i['reg','KR',1]) , axis = 1)

And_Cohort_Counrty_Data = And_Cohort_Counrty_Data.ix[ :, And_Cohort_Counrty_Data.columns.get_level_values(2).isin({"3 LTV", "7 LTV", "KPI: 7 LTV", "次留"})]


## iOS 市场周数据
iOS_Cohort_Counrty = str(
    '''
        SELECT 
            a.Date, a.registerDate, a.xday, registerAreaCode,
            sum( a.loginCnt1) as login, sum( a.registerCnt1) as reg, 
            round( sum( a.paySum1)*0.49,2) as Recharge 
        FROM
        (
        	SELECT
        		Date, registerDate, DATEDIFF( Date, registerDate) +1 as xday, advertiser, countryGroup, registerAreaCode,
        		IFNULL( loginCnt,0) as loginCnt1, IFNULL( registerCnt,0) as registerCnt1, IFNULL( paySum,0) as paySum1
        	FROM
        		`t_data_track`
        	WHERE
        		registerDate BETWEEN '%s' AND '%s'
        	AND Date BETWEEN '%s' AND '%s'
        	AND advertiser != 'untrusted devices'  ## 假量
          AND platform = 'iOS'
        	AND gameCode = 'stgl'
          AND tier = 'T1'
        ) a
        GROUP BY a.Date, a.registerDate, a.xday, registerAreaCode
        HAVING a.xday <= 7;
''' %( date_calculate()[0], date_calculate()[1], date_calculate()[0], date_calculate()[1])) 

iOS_Cohort_Counrty_Data = pd.read_sql( iOS_Cohort_Counrty, con = connection)
iOS_Cohort_Counrty_Data = pd.merge( iOS_Cohort_Counrty_Data, Week_config, left_on = 'registerDate', right_on = 'Date')

iOS_Cohort_Counrty_Data = pd.pivot_table( iOS_Cohort_Counrty_Data, index = ['Class'], columns=['registerAreaCode', 'xday'], values = ['reg','login', 'Recharge'])

iOS_Cohort_Counrty_Data = iOS_Cohort_Counrty_Data.fillna(0)


## T1 -EN 
iOS_Cohort_Counrty_Data['iOS数据', '1. T1-EN', '3 LTV'] = iOS_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','AU',1]+ i['Recharge','CA',1]+ i['Recharge','US',1]+ i['Recharge','GB',1]+
                    i['Recharge','AU',2]+ i['Recharge','CA',2]+ i['Recharge','US',2]+ i['Recharge','GB',2]+
                    i['Recharge','AU',3]+ i['Recharge','CA',3]+ i['Recharge','US',3]+ i['Recharge','GB',3])/( i['reg','AU',1]+ i['reg','CA',1]+ i['reg','US',1]+ i['reg','GB',1]) , axis = 1)

iOS_Cohort_Counrty_Data['iOS数据', '1. T1-EN', '7 LTV'] = iOS_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','AU',1]+ i['Recharge','CA',1]+ i['Recharge','US',1]+ i['Recharge','GB',1]+
                    i['Recharge','AU',2]+ i['Recharge','CA',2]+ i['Recharge','US',2]+ i['Recharge','GB',2]+
                    i['Recharge','AU',3]+ i['Recharge','CA',3]+ i['Recharge','US',3]+ i['Recharge','GB',3]+
                    i['Recharge','AU',4]+ i['Recharge','CA',4]+ i['Recharge','US',4]+ i['Recharge','GB',4]+
                    i['Recharge','AU',5]+ i['Recharge','CA',5]+ i['Recharge','US',5]+ i['Recharge','GB',5]+
                    i['Recharge','AU',6]+ i['Recharge','CA',6]+ i['Recharge','US',6]+ i['Recharge','GB',6]+
                    i['Recharge','AU',7]+ i['Recharge','CA',7]+ i['Recharge','US',7]+ i['Recharge','GB',7] )/( i['reg','AU',1]+ i['reg','CA',1]+ i['reg','US',1]+ i['reg','GB',1]) , axis = 1)

iOS_Cohort_Counrty_Data['iOS数据', '1. T1-EN', 'KPI: 7 LTV'] = ''

iOS_Cohort_Counrty_Data['iOS数据', '1. T1-EN', '次留'] = iOS_Cohort_Counrty_Data.apply( 
        lambda i: ( i['login','AU',2]+ i['login','CA',2]+ i['login','US',2]+ i['login','GB',2])/( i['reg','AU',1]+ i['reg','CA',1]+ i['reg','US',1]+ i['reg','GB',1]) , axis = 1)

## DE 
iOS_Cohort_Counrty_Data['iOS数据', '2. DE', '3 LTV'] = iOS_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','DE',1]+ i['Recharge','DE',2]+ i['Recharge','DE',3] )/( i['reg','DE',1]) , axis = 1)

iOS_Cohort_Counrty_Data['iOS数据', '2. DE', '7 LTV'] = iOS_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','DE',1]+ i['Recharge','DE',2]+ i['Recharge','DE',3]+ i['Recharge','DE',4]+ 
                    i['Recharge','DE',5]+ i['Recharge','DE',6]+ i['Recharge','DE',7] )/( i['reg','DE',1]) , axis = 1)

iOS_Cohort_Counrty_Data['iOS数据', '2. DE', 'KPI: 7 LTV'] = ''

iOS_Cohort_Counrty_Data['iOS数据', '2. DE', '次留'] = iOS_Cohort_Counrty_Data.apply( 
        lambda i: ( i['login','DE',2])/( i['reg','DE',1]) , axis = 1)

## RU
iOS_Cohort_Counrty_Data['iOS数据', '3. RU', '3 LTV'] = iOS_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','RU',1]+ i['Recharge','RU',2]+ i['Recharge','RU',3] )/( i['reg','RU',1]) , axis = 1)

iOS_Cohort_Counrty_Data['iOS数据', '3. RU', '7 LTV'] = iOS_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','RU',1]+ i['Recharge','RU',2]+ i['Recharge','RU',3]+ i['Recharge','RU',4]+ 
                    i['Recharge','RU',5]+ i['Recharge','RU',6]+ i['Recharge','RU',7] )/( i['reg','RU',1]) , axis = 1)

iOS_Cohort_Counrty_Data['iOS数据', '3. RU', 'KPI: 7 LTV'] = ''

iOS_Cohort_Counrty_Data['iOS数据', '3. RU', '次留'] = iOS_Cohort_Counrty_Data.apply( 
        lambda i: ( i['login','RU',2])/( i['reg','RU',1]) , axis = 1)

## HK/TW
iOS_Cohort_Counrty_Data['iOS数据', '4. HK/TW', '3 LTV'] = iOS_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','HK',1]+ i['Recharge','TW',1]+
                    i['Recharge','HK',2]+ i['Recharge','TW',2]+
                    i['Recharge','HK',3]+ i['Recharge','TW',3])/( i['reg','HK',1]+ i['reg','TW',1]) , axis = 1)

iOS_Cohort_Counrty_Data['iOS数据', '4. HK/TW', '7 LTV'] = iOS_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','HK',1]+ i['Recharge','TW',1]+
                    i['Recharge','HK',2]+ i['Recharge','TW',2]+
                    i['Recharge','HK',3]+ i['Recharge','TW',3]+
                    i['Recharge','HK',4]+ i['Recharge','TW',4]+
                    i['Recharge','HK',5]+ i['Recharge','TW',5]+
                    i['Recharge','HK',6]+ i['Recharge','TW',6]+
                    i['Recharge','HK',7]+ i['Recharge','TW',7])/( i['reg','HK',1]+ i['reg','TW',1]) , axis = 1)

iOS_Cohort_Counrty_Data['iOS数据', '4. HK/TW', 'KPI: 7 LTV'] = ''

iOS_Cohort_Counrty_Data['iOS数据', '4. HK/TW', '次留'] = iOS_Cohort_Counrty_Data.apply( 
        lambda i: ( i['login','HK',2]+ i['login','TW',2])/( i['reg','JP',1]+ i['reg','KR',1]) , axis = 1)

## JP/KR
iOS_Cohort_Counrty_Data['iOS数据', '5. JP/KR', '3 LTV'] = iOS_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','JP',1]+ i['Recharge','KR',1]+
                    i['Recharge','JP',2]+ i['Recharge','KR',2]+
                    i['Recharge','JP',3]+ i['Recharge','KR',3])/( i['reg','JP',1]+ i['reg','KR',1]) , axis = 1)

iOS_Cohort_Counrty_Data['iOS数据', '5. JP/KR', '7 LTV'] = iOS_Cohort_Counrty_Data.apply( 
        lambda i: ( i['Recharge','JP',1]+ i['Recharge','KR',1]+
                    i['Recharge','JP',2]+ i['Recharge','KR',2]+
                    i['Recharge','JP',3]+ i['Recharge','KR',3]+
                    i['Recharge','JP',4]+ i['Recharge','KR',4]+
                    i['Recharge','JP',5]+ i['Recharge','KR',5]+
                    i['Recharge','JP',6]+ i['Recharge','KR',6]+
                    i['Recharge','JP',7]+ i['Recharge','KR',7])/( i['reg','JP',1]+ i['reg','KR',1]) , axis = 1)

iOS_Cohort_Counrty_Data['iOS数据', '5. JP/KR', 'KPI: 7 LTV'] = ''

iOS_Cohort_Counrty_Data['iOS数据', '5. JP/KR', '次留'] = iOS_Cohort_Counrty_Data.apply( 
        lambda i: ( i['login','JP',2]+ i['login','KR',2])/( i['reg','JP',1]+ i['reg','KR',1]) , axis = 1)

iOS_Cohort_Counrty_Data = iOS_Cohort_Counrty_Data.ix[ :, iOS_Cohort_Counrty_Data.columns.get_level_values(2).isin({"3 LTV", "7 LTV", "KPI: 7 LTV", "次留"})]

## ------ 早会数据 ------
Meeting = pd.DataFrame( columns =  ['Date', 'Recharge', 'Pay_Rate', 'ARPPU',  'DNU', 'DAU', 'EDAU',
                                    '安卓次留', 'iOS次留', '1自然量LTV', '3LTV',  '7LTV', 'CPU', 'COST',
                                    '3ROI', '7ROI', 'And_Cost', 'iOS_Cost',  'Pay_Num'])
## 运营数据
Meeting['Date'] = And_actual_data[['Date']]
Meeting['Recharge'] = And_actual_data['Recharge'] + iOS_actual_data['Recharge']
Meeting['DNU'] = And_actual_data['DNU'] + iOS_actual_data['DNU']
Meeting['DAU'] = And_actual_data['DAU'] + iOS_actual_data['DAU']
Meeting['Pay_Num'] = And_actual_data['Pay_Num'] + iOS_actual_data['Pay_Num']
## 留存
Meeting['安卓次留'] = pd.DataFrame( list( And_Cohort_Data['留存', '次留']))
Meeting['iOS次留'] = pd.DataFrame( list( iOS_Cohort_Data['留存', '次留']))
## LTV
Meeting['1自然量LTV'] = pd.DataFrame( 
        list( 
                Whole_Cohoe_Data.apply( lambda i: (i['Recharge', '自然量', 1])/i['reg', '自然量', 1] , axis = 1)
                )
        )
Meeting['3LTV'] = pd.DataFrame( list( Whole_Cohoe_Data['3LTV', '整体', '3 LTV1']))
Meeting['7LTV'] = pd.DataFrame( list( Whole_Cohoe_Data['7LTV', '整体', '7 LTV1']))


## ------ Output ------
## Operation
output = str( r"C:\Users\Efun\Desktop\日报基础数据（产品） - %s.xlsx" % (date_calculate()[1]) )
writer = pd.ExcelWriter( output)

And_actual_data.to_excel( writer, index = False, encoding = 'utf-8', sheet_name = 'And_actual')
iOS_actual_data.to_excel( writer, index = False, encoding = 'utf-8', sheet_name = 'iOS_actual')
And.to_excel( writer, encoding = 'utf-8', sheet_name = 'And_dnu')
iOS.to_excel( writer, encoding = 'utf-8', sheet_name = 'iOS_dnu')
And_Cohort_Data_Op.to_excel( writer, encoding = 'utf-8', sheet_name = 'And_Cohort')
iOS_Cohort_Data_Op.to_excel( writer, encoding = 'utf-8', sheet_name = 'iOS_Cohort')
Whole_Cohoe_Data1.to_excel( writer, encoding = 'utf-8', sheet_name = '整体留存')
Whole_Cohoe_Data2.to_excel( writer, encoding = 'utf-8', sheet_name = '整体LTV')
Meeting.to_excel( writer, index = False, encoding = 'utf-8', sheet_name = '早会数据')

writer.save()

## Market
output = str( r"C:\Users\Efun\Desktop\日报基础数据（市场） - %s.xlsx" % (date_calculate()[1]) )
writer = pd.ExcelWriter( output)

And_Cohort_Data_Market.to_excel( writer, encoding = 'utf-8', sheet_name = '日报-Android')
iOS_Cohort_Data_Market.to_excel( writer, encoding = 'utf-8', sheet_name = '日报-iOS')
And_Market.to_excel( writer, encoding = 'utf-8', sheet_name = 'DNU监控-Android')
iOS_Market.to_excel( writer, encoding = 'utf-8', sheet_name = 'DNU监控-iOS')
And_Cohort_Counrty_Data.to_excel( writer, encoding = 'utf-8', sheet_name = 'LTV监控-分地区-Android')
iOS_Cohort_Counrty_Data.to_excel( writer, encoding = 'utf-8', sheet_name = 'LTV监控-分地区-iOS')

writer.save()
