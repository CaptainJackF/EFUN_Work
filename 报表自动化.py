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


## ------ 留存Cohor ------
And_Cohor = str(
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

And_Cohoe_Data = pd.read_sql( And_Cohor, con = connection)
And_Cohoe_Data = pd.pivot_table( And_Cohoe_Data, index = ['registerDate'], columns=['xday'], values = ['reg','login', 'Recharge'])
And_Cohoe_Data = And_Cohoe_Data.fillna(0)

And_Cohoe_Data['留存', 'DNU'] = And_Cohoe_Data['reg', 1]
And_Cohoe_Data['留存', '次留'] = And_Cohoe_Data.apply( lambda i: i['login', 2]/i['reg', 1] , axis = 1)
And_Cohoe_Data['留存', '7留'] = And_Cohoe_Data.apply( lambda i: i['login', 7]/i['reg', 1] , axis = 1)
And_Cohoe_Data['留存', 'CPU'] = ''

And_Cohoe_Data['LTV', '3 LTV'] = And_Cohoe_Data.apply( 
        lambda i: '-' if i['Recharge', 3] == 0 else (i['Recharge', 1]+i['Recharge', 2]+i['Recharge', 3])/i['reg', 1] , axis = 1)
And_Cohoe_Data['LTV', '3 ROI'] = ''
And_Cohoe_Data['LTV', '7 LTV'] = And_Cohoe_Data.apply( 
        lambda i: '-' if i['Recharge', 7] == 0 else (i['Recharge', 1]+i['Recharge', 2]+i['Recharge', 3]+i['Recharge', 4]+i['Recharge', 5]+i['Recharge', 6]+i['Recharge', 7])/i['reg', 1] , axis = 1)

And_Cohoe_Data = And_Cohoe_Data.ix[ :, And_Cohoe_Data.columns.get_level_values(1).isin({"DNU", "次留", "7留", "CPU", "3 LTV", "3 ROI", "7 LTV"})]


iOS_Cohor = str(
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

iOS_Cohoe_Data = pd.read_sql( iOS_Cohor, con = connection)
iOS_Cohoe_Data = pd.pivot_table( iOS_Cohoe_Data, index = ['registerDate'], columns=['xday'], values = ['reg','login', 'Recharge'])
iOS_Cohoe_Data = iOS_Cohoe_Data.fillna(0)

iOS_Cohoe_Data['留存', 'DNU'] = iOS_Cohoe_Data['reg', 1]
iOS_Cohoe_Data['留存', '次留'] = iOS_Cohoe_Data.apply( lambda i: i['login', 2]/i['reg', 1] , axis = 1)
iOS_Cohoe_Data['留存', '7留'] = iOS_Cohoe_Data.apply( lambda i: i['login', 7]/i['reg', 1] , axis = 1)
iOS_Cohoe_Data['留存', 'CPU'] = ''

iOS_Cohoe_Data['LTV', '3 LTV'] = iOS_Cohoe_Data.apply( 
        lambda i: '-' if i['Recharge', 3] == 0 else (i['Recharge', 1]+i['Recharge', 2]+i['Recharge', 3])/i['reg', 1] , axis = 1)
iOS_Cohoe_Data['LTV', '3 ROI'] = ''
iOS_Cohoe_Data['LTV', '7 LTV'] = iOS_Cohoe_Data.apply( 
        lambda i: '-' if i['Recharge', 7] == 0 else (i['Recharge', 1]+i['Recharge', 2]+i['Recharge', 3]+i['Recharge', 4]+i['Recharge', 5]+i['Recharge', 6]+i['Recharge', 7])/i['reg', 1] , axis = 1)


iOS_Cohoe_Data = iOS_Cohoe_Data.ix[ :, iOS_Cohoe_Data.columns.get_level_values(1).isin({"DNU", "次留", "7留", "CPU", "3 LTV", "3 ROI", "7 LTV"})]



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


## ------ Output ------
writer = pd.ExcelWriter( r'D:\Work\201807\20180707 - 报表自动化\Test.xlsx')

And_actual_data.to_excel( writer, index = False, encoding = 'utf-8', sheet_name = 'And_actual')
iOS_actual_data.to_excel( writer, index = False, encoding = 'utf-8', sheet_name = 'iOS_actual')
And.to_excel( writer, encoding = 'utf-8', sheet_name = 'And_dnu')
iOS.to_excel( writer, encoding = 'utf-8', sheet_name = 'iOS_dnu')
And_Cohoe_Data.to_excel( writer, encoding = 'utf-8', sheet_name = 'And_Cohoe')
iOS_Cohoe_Data.to_excel( writer, encoding = 'utf-8', sheet_name = 'iOS_Cohoe')
Whole_Cohoe_Data1.to_excel( writer, encoding = 'utf-8', sheet_name = '整体留存')
Whole_Cohoe_Data2.to_excel( writer, encoding = 'utf-8', sheet_name = '整体LTV')

writer.save()