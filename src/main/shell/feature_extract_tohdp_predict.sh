#!/bin/bash
# *****************************************************************
# name:    金融scala模型特征获取+迁移+训练的定时脚本
# 参数解释： 因为整个流程比较长，所以date_id需要固定，不然后期无法匹配 sh
# 输入表：   XXX
# 输出表：   ubd_ana_test_2.dwa_d_finance_cust_marketing_{loan/creditcard}_{will/connect} partition(date_id
# 执行周期： 每周六凌晨启动一次 crontab 00 2 * * 6
# 主逻辑:   1 登陆319 执行candidate_319.sql 得到账期内candidate表 ubd_ana_test_2.dwa_d_finance_cust_marketing_candidate_use
#          2 根据1 从319、419获取特征csv
#          3 将finance_marketing_feature.csv finance_marketing_feature_recent_visit.csv 传入HDFS
#          4 用新账期的特征进行预测 creditcard loan
#          5 将预测结果存入319的hive表
# 注：
# *****************************************************************

source /opt/beh/conf/hdp_env
# *****************************************************************
#              定义时间变量
# *****************************************************************
# 当前时间
v_now_time=$(date +"%Y%m%d %H:%M:%S")
# 直接输入固定日期/账期
date_id=$(echo "$v_now_time" | cut -c 1-8)
# 删除文件夹的日期
delete_date=$(date -d "$date_id -14 day" +%Y%m%d)

# *****************************************************************
#              定义路径、文件名等
# *****************************************************************
# 集群地址
ip_319='10.191.21.11'
ip_419='10.162.161.105'
# hdp程序地路径 用户：project_user
project_user="limy" ####用户在这里修改
marketing_path="/data01/ubd_obx_test/${project_user}/chinaunicom_finance_marketing_distributed_adv/src/main/shell/"
# 319、419特征地址
target_path="/data/ubd_ana_test/tx/projects_data/all_user/"
# hdp
local_path="/data01/ubd_obx_test/finance/all_user/"
hadoop_path="/tmp/finance/all_user/"
# 319程序路径 用户：project_user
project_path="/data/ubd_ana_test/${project_user}/chinaunicom_finance_marketing/.idea/src/marketing_projects/all_user/"

# *****************************************************************
#     319、419特征提取
# *****************************************************************
ssh -t -t ubd_ana_test@${ip_319} << EOF

cd ${project_path}
#提取账期内candidate到表 是否需要检查一下有没有数
sh candidate_319.sh ${date_id}
echo "-------candidate_319 done-------"

#提取特征 319&419
sh feature_extract_319.sh ${date_id}
exit
EOF
echo "-------feature extract done-------"

# *****************************************************************
#     将账期内的特征迁移到hdp集群，并上传至hdfs
# *****************************************************************
rm -rf ${local_path}${delete_date}
mkdir ${local_path}${date_id}
# 把319的feature传到hdp集群上
scp ubd_ana_test@${ip_319}:${target_path}finance_marketing_feature.csv ${local_path}${date_id}
echo "-------319 scp done-------"

# 把419的feature传到hdp集群上
scp ubd_ana_test@${ip_419}:${target_path}finance_marketing_feature_recent_visit.csv ${local_path}${date_id}
echo "-------419 scp done-------"

# 上传hdfs
hadoop fs -rm -r ${hadoop_path}${delete_date}
hadoop fs -mkdir ${hadoop_path}${date_id}
hadoop fs -put ${local_path}${date_id}/finance_marketing_feature.csv ${hadoop_path}${date_id}
hadoop fs -put ${local_path}${date_id}/finance_marketing_feature_recent_visit.csv ${hadoop_path}${date_id}
echo "-------feature put to hdfs----------"

# *****************************************************************
#     读取HDFS文件信息并写入文件，计算每小时文件数和数据量
# *****************************************************************
# predict creditcard
cd ${marketing_path}
sh makePrediction_creditcard.sh ${date_id}
sh makePrediction_loan.sh ${date_id}
echo "-------prediction done----------"

# *****************************************************************
#     将预测结果存表--319
# *****************************************************************
sh getResScp_hdp.sh loan will ${date_id}
sh getResScp_hdp.sh loan connect ${date_id}
sh getResScp_hdp.sh creditcard will ${date_id}
sh getResScp_hdp.sh creditcard connect ${date_id}
echo "-------save table done----------"

# *****************************************************************
#     检查319表里数是否有数并推数
# *****************************************************************
ssh -t -t ubd_ana_test@${ip_319} << EOF

cd /data/ubd_ana_test/limy/scalaFinance
# 检查预测数据是否成功存表，并打印去重合计量
sh checkTable.sh ${date_id}
cat checklog_${date_id}.log
echo "-------check table done-------"

# 推数
cd ${project_path}
sh send.sh ${date_id}
exit
EOF
echo "-------send done-------"

# *****************************************************************
#     计算整个流程需要的时间
# *****************************************************************
v_end_time=$(date +"%Y%m%d %H:%M:%S")
start_seconds=$(date --date="$v_now_time" +%s)
end_seconds=$(date --date="$v_end_time" +%s)
echo "------------当前时间：$v_end_time,整个流程耗时："$((end_seconds - start_seconds))"s------------"
