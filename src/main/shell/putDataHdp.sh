#!/bin/bash

####把all_user特征文件scp到hdp集群并put到指定目录####
ip_1=
ip_2=
date_id=$1
target_path=""
local_path=""
hadoop_path=""
mkdir ${local_path}${date_id}

#把319的feature传到hdp上
scp ubd_ana_test@${ip_1}:${target_path}finance_marketing_feature.csv ${local_path}${date_id}
echo "IP1 scp done"
#把419的feature传到hdp上
scp ubd_ana_test@${ip_2}:${target_path}finance_marketing_feature_recent_visit.csv ${local_path}${date_id}
echo "IP2 scp done"

#put feature to hdp hadoop
hadoop fs -mkdir ${hadoop_path}${date_id}
hadoop fs -put ${local_path}${date_id}/finance_marketing_feature.csv ${hadoop_path}${date_id}
hadoop fs -put ${local_path}${date_id}/finance_marketing_feature_recent_visit.csv ${hadoop_path}${date_id}