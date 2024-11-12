#!/bin/bash

product=$1
scene=$2
proc_date=$3

#### various
hdfs_path="/tmp/finance/all_user/"${product}_${scene}_${proc_date}
local_path="/data01/ubd_obx_test/tx/projects_data/all_user/"${product}_${scene}_${proc_date}
target_path="/data/ubd_ana_test/tx/projects_data/all_user/"${product}_${scene}_${proc_date}

ip_319='10.191.21.11'

#### res scp
hadoop fs -get ${hdfs_path} ${local_path}
echo "get succeed"

scp -r ${local_path} ubd_ana_test@${ip_319}:${target_path}
echo "scp succeed"

to_run="/data/ubd_ana_test/tx/chinaunicom_finance_marketing_distributed_adv/src/main/shell/loadInTable.sql"
ssh ubd_ana_test@${ip_319} << EOF
source /opt/beh/conf/beh_env;
hive --hivevar data_path=${target_path} \
     --hivevar product=${product} \
     --hivevar scene=${scene} \
     --hivevar proc_date=${proc_date} \
     -f ${to_run}
EOF


