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

--入表
load data local inpath '${hivevar:data_path}'
into table ubd_ana_test_2.dwa_d_finance_cust_marketing_${hivevar:product}_${hivevar:scene} partition(date_id=${hivevar:proc_date});
