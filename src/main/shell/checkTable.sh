#!/bin/bash

proc_date=$1
hive -e "
SET mapreduce.job.queuename=xl_non_production;
SET mapred.job.name=ubd_jsb_limy;
SELECT
    COUNT(DISTINCT device_number)
FROM
    ubd_ana_test_2.dwa_d_finance_cust_marketing_loan_connect
WHERE
    date_id=${proc_date};
">/data/ubd_ana_test/limy/scalaFinance/checklog_${proc_date}.log

hive -e "
SET mapreduce.job.queuename=xl_non_production;
SET mapred.job.name=ubd_jsb_limy;
SELECT
    COUNT(DISTINCT device_number)
FROM
    ubd_ana_test_2.dwa_d_finance_cust_marketing_loan_will
WHERE
    date_id=${proc_date};
">>/data/ubd_ana_test/limy/scalaFinance/checklog_${proc_date}.log

hive -e "
SET mapreduce.job.queuename=xl_non_production;
SET mapred.job.name=ubd_jsb_limy;
SELECT
    COUNT(DISTINCT device_number)
FROM
    ubd_ana_test_2.dwa_d_finance_cust_marketing_creditcard_connect
WHERE
    date_id=${proc_date}
">>/data/ubd_ana_test/limy/scalaFinance/checklog_${proc_date}.log

hive -e "
SET mapreduce.job.queuename=xl_non_production;
SET mapred.job.name=ubd_jsb_limy;
SELECT
    COUNT(DISTINCT device_number)
FROM
    ubd_ana_test_2.dwa_d_finance_cust_marketing_creditcard_will
WHERE
    date_id=${proc_date}
">>/data/ubd_ana_test/limy/scalaFinance/checklog_${proc_date}.log