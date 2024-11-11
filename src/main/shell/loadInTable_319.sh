#!/bin/bash
source /opt/beh/conf/beh_env

proc_date=$(date +%Y%m%d -d " -4 day")

####变量
##场景
predict_type="loan"
##scp的文件夹地址
path_319="limy/financeSpark"
##目标文件夹
file_path="/data/ubd_ana_test/${path_319}/all_user"

####
hive --hivevar file_path=${file_path} \
     --hivevar predict_type=${predict_type} \
     --hivevar proc_date=${proc_date} \
    -f loadInTable.sql