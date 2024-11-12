#!/bin/bash

product=$1
scene=$2
proc_date=$3

#### various
hdfs_path=""
local_path=""
target_path=""

ip_1=""

#### res scp
hadoop fs -get ${hdfs_path} ${local_path}
echo "get succeed"

scp -r ${local_path} XXX@${ip_1}:${target_path}
echo "scp succeed"

to_run="XXX/loadInTable.sql"
ssh XXX@${ip_1} << EOF
source /opt/beh/conf/beh_env;
hive --hivevar data_path=${target_path} \
     --hivevar product=${product} \
     --hivevar scene=${scene} \
     --hivevar proc_date=${proc_date} \
     -f ${to_run}
EOF


