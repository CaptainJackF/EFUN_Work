library( dplyr)
library( readxl)
library( data.table)
library( reshape2)

n = 1  ## 需要统计的天数
year <- format( Sys.Date(), "%Y")
month <- format( Sys.Date(), "%m")
day <- format( Sys.Date(), "%d")  # 返回字符串格式

end_date <- ISOdatetime( year, month, as.numeric( day) - 1, 23,59,59)  ## 构造前一天的23:59:59
start_date <- end_date - 24*3600*n   ## 计算开始时间, 用于后续筛选

setwd( "C:\\Users\\Efun\\Desktop")

efun <- fread( "Efun充值列表.csv", stringsAsFactors = F)　
config <- read_excel( "D:\\Work\\日报\\礼包甘特图\\礼包甘特图模板.xlsx",
                      sheet = "Sheet2")
config_rank <- read_excel( "D:\\Work\\日报\\礼包甘特图\\礼包甘特图模板.xlsx",
                           sheet = "Sheet3")


efun$`北京时间` <- as.POSIXct( strptime( efun$`充值时间`, "%Y-%m-%d %H:%M")) + 8*60*60

efun_result <- filter( efun,
                       `北京时间` > start_date,
                       `北京时间` <= end_date,
                       `状态` == 1) %>%
  mutate( Date = format( `北京时间`, "%Y/%m/%d")) %>%
  select( `原厂产品Id`, `充值金额`, Date) %>%
  left_join( config,
             by = "原厂产品Id") %>%
  group_by( Date, `标签`) %>%
  summarise( recharge = sum( `充值金额`)) 


result <- left_join( config_rank, efun_result,
                     by = "标签") %>%
  dcast( 标签 + 排序 ~ Date) %>%
  arrange( 排序)

result[ is.na( result)] <- ""

write.csv( result, "甘特图统计结果.csv", row.names = F)
