                                                                                --loan
--419队列名称
--set mapreduce.job.queuename=ia_ana;
--319队列
set mapreduce.job.queuename=xl_non_production;
--设置任务名
set mapred.job.name=udb-jsb-tx;

set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=134217728;
set hive.merge.size.per.task=536870912;
set mapred.max.split.size=1073741824;
set mapred.min.split.size.per.node=1073741824;
set mapred.min.split.size.per.rack=1073741824;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.hadoop.supports.splittable.combineinputformat=true;
set hive.cli.print.header=true;
set hive.resultset.use.unique.column.names=false;


drop table if exists ubd_ana_test_2.dwa_d_finance_cust_marketing_creditcard_will;
CREATE TABLE if not exists ubd_ana_test_2.dwa_d_finance_cust_marketing_creditcard_will(
  `device_number` string,
  `score` double
)
partitioned by (
date_id string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim'=',',
  'serialization.format'=',')
TBLPROPERTIES('author'='tx');

drop table if exists ubd_ana_test_2.dwa_d_finance_cust_marketing_creditcard_connect;
CREATE TABLE if not exists ubd_ana_test_2.dwa_d_finance_cust_marketing_creditcard_connect(
  `device_number` string,
  `score` double
)
partitioned by (
date_id string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim'=',',
  'serialization.format'=',')
TBLPROPERTIES('author'='tx');

drop table if exists ubd_ana_test_2.dwa_d_finance_cust_marketing_loan_will;
CREATE TABLE if not exists ubd_ana_test_2.dwa_d_finance_cust_marketing_loan_will(
  `device_number` string,
  `score` double
)
partitioned by (
date_id string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim'=',',
  'serialization.format'=',')
TBLPROPERTIES('author'='tx');

drop table if exists ubd_ana_test_2.dwa_d_finance_cust_marketing_loan_connect;
CREATE TABLE if not exists ubd_ana_test_2.dwa_d_finance_cust_marketing_loan_connect(
  `device_number` string,
  `score` double
)
partitioned by (
date_id string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim'=',',
  'serialization.format'=',')
TBLPROPERTIES('author'='tx');