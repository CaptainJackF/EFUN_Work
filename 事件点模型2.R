library( readxl)
library( dplyr)
library( ggplot2)

## 设置工作路径
setwd( "D:\\Work\\201811\\20181117 - 事件点模型\\Data")

## 读取目标用户列表
Ori_dataset <- tbl_df( 
  read_excel( "D:\\Work\\201811\\20181117 - 事件点模型\\目标用户.xlsx", "Sheet")
)

## 样本平衡性: 不平衡，需要最忌抽样
prop.table( table( Ori_dataset$user_type))

## 读取特征数据表名list
file_list <- list.files( ".", pattern = "xlsx")

## 循环遍历
for( i in file_list){
  
  ## 读取临时数据
  temp_file  <- tbl_df( 
    read_excel( i, "Sheet")
  )
  
  Ori_dataset <- tbl_df(
    left_join( Ori_dataset, temp_file, 
               by = c( "server_id", "user_id", "user_type"),
               all = TRUE)
  )
  
  remove( temp_file)
}



## 排除掉市政厅等级为 NA 或 < 3 级的用户, 并
Ori_dataset <- filter( Ori_dataset, !is.na(`市政厅等级`) ) %>%
  filter( `市政厅等级` >= 3 ) %>%
  mutate( 
    `用户分类` = user_type,
    `首次付费时间` =  as.numeric( difftime( as.Date( 首次付费时间, format = "%Y-%m-%d"), as.Date( reg_day, format = "%Y-%m-%d"), units = "days"))+1
    ) %>%
  select( -one_of( c( "server_id","user_id","reg_day","last_login_time","date_diff","user_type","首次付费礼包")))

#str( Ori_dataset)

write.csv( Ori_dataset, Ori_Testing
           "D:\\Work\\201811\\20181117 - 事件点模型\\目标人群特征(Backup).csv", row.names = F)

Ori_dataset <- tbl_df( 
  read.csv( "D:\\Work\\201811\\20181117 - 事件点模型\\目标人群特征.csv", stringsAsFactors = FALSE) 
)

## 缺失值处理
library( mice)
miss_value <- md.pattern( Ori_dataset)  # 返回数据的缺失值的模式

  ## 登录情况: 2000+条缺失值，但这批用户可能是2-3天正好未登录
table( is.na( Ori_dataset$X17 ))
# Ori_dataset <- filter( Ori_dataset, !is.na(`登录次数`) ) 
Ori_dataset[ ,c('X17')][ is.na( Ori_dataset[ ,c('X17')])] <- 0   # 先将缺失值设为0，防止下列剔除噪点时删掉这部分有用的数据
Ori_dataset <- filter( Ori_dataset, X17 <= 100 ) %>% # 剔除登录次数
  mutate( X17 = ifelse( X17 == 0, NA, X17))

# 密度曲线
ggplot( Ori_dataset, aes( x = X17, colour = Y)) + geom_density() # 添加密度曲线

# 插补
full_1 <- select( Ori_dataset,
                  X17, X18, X4, X5, X8, X9, X22, X23, X24, X25, X26, X27, X28, Y)
set.seed( 1000)
tempData <- mice( full_1, seed = 1000)
mice_output <- complete( tempData) ## 查看完整数据

Ori_dataset$X17 <- mice_output$X17
Ori_dataset$X18 <- mice_output$X18

  ## 替换缺失值, 除了登录情况外，其余缺失值可认为是未完成
Ori_dataset[ is.na( Ori_dataset)] <- 0 

## 异常值处理


## 特征相关性
library( Hmisc)#加载包
library( corrplot)#先加载包
res <- round( cor( Ori_dataset), 2)
res2 <- rcorr( as.matrix( Ori_dataset))
res2$r

corrplot( res, type = "upper", order = "hclust", tl.col = "black", tl.srt = 45)

as.Date( Ori_dataset$首次付费时间,format = "%Y-%m-%d")


table(is.na( Ori_dataset))
table(is.na( Ori_dataset))

## 逻辑回归

Ori_dataset$`用户分类` <- factor( Ori_dataset$`用户分类`, order = TRUE)
model <- glm( `用户分类`~.,data = Ori_dataset, family = "binomial")
model1 <- glm( `用户分类`~联盟职位 + 是否有多个英雄 + 英雄等级 + 首次付费时间 + 
                 VIP等级 + 贤者之石采集次数 + 登录次数,
              data = Ori_dataset, family = "binomial")
summary( model1)

## 卡方检验
anova( model, model1, test = "Chisq")

coef( model1)
exp( coef( model1))

exp( confint( model1)) 

pre <- predict( model1, Ori_dataset)


## 模型评估
library( pROC)

pre <- predict( model1, Ori_dataset)
modelroc <- roc( Ori_dataset$`用户分类`, pre)
plot( modelroc, print.auc = TRUE, auc.polygon = TRUE, grid = c(0.1, 0.2),
     grid.col = c("green", "red"), max.auc.polygon = TRUE,
     auc.polygon.col = "skyblue", print.thres = TRUE)





## Random Forest
library(randomForest)
library(ggplot2)

n <-length( names( Ori_dataset))     #计算数据集中自变量个数，等同n=ncol(train_data)
rate = 1     #设置模型误判率向量初始值


  set.seed( 1234)
  rf_train <- randomForest( as.factor( Ori_dataset$user_type)~., data = Ori_dataset, mtry = i, ntree = 1000)


plot( rarf_trainte)
