library( readxl) ## read excel file
library( dplyr) ## data manipulation


setwd( "C:\\Users\\Efun\\Desktop\\20180525 - 科技坑位受兵种投放影响")

config <- read_excel( "新建 Microsoft Excel 工作表.xlsx",
                      sheet = "1121科技快照")

config <- filter( config,
                  `分类` == ">=300") %>%
  select( level_city, '领主科技等级','内政科技等级','基础军备等级','高级军备1等级',
          '高级军备2等级', '资源科技1等级', '资源科技2等级') 

x <- aggregate( config, by = list( config$level_city), FUN = median)
