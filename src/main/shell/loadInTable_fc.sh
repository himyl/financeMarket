#!/bin/bash
source /opt/beh/conf/hdp_env

proc_date=$(date +%Y%m%d -d " ")
pj_name="creditcard"
scene="will"

###############################################################################
##namenode判活
HDFS_CODE_hdp=hdfs://10.191.21.10
HDFS_CODE_319=hdfs://10.191.21.11




####对拷数据
hadoop distcp -Dmapred.job.queue.name=ia_xl_pro -overwrite -delete -m 200 -bandwidth 1000 ${HDFS_CODE_hdp}/tmp/finance/all_user/loan_20220510 ${HDFS_CODE_319}/tmp/limy/all_user

exit；
s11
scp -r ubd_obx_test@10.191.21.10:/data01/ubd_obx_test/limy/all_user/${pj_name}/

####人工上传
load data local inpath '/data/ubd_ana_test/limy/all_user/creditcard_connect_20220510'
into table ubd_ana_test_2.dwa_d_finance_cust_marketing_creditcard_connect partition (date_id='20220510');

load data local inpath '/data/ubd_ana_test/limy/all_user/creditcard_will_20220510'
into table ubd_ana_test_2.dwa_d_finance_cust_marketing_creditcard_will partition (date_id='20220510');

load data local inpath '/data/ubd_ana_test/limy/all_user/loan_will_20220510'
into table ubd_ana_test_2.dwa_d_finance_cust_marketing_loan_will partition (date_id='20220510');

load data local inpath '/data/ubd_ana_test/limy/all_user/loan_connect_20220510'
into table ubd_ana_test_2.dwa_d_finance_cust_marketing_loan_connect partition (date_id='20220510');