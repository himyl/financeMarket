
set mapreduce.job.queuename=XXXXX;
set mapred.job.name=XXXXX;

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

--load table
load data local inpath '${hivevar:data_path}'
into table ubd_ana_test_2.dwa_d_finance_cust_marketing_${hivevar:product}_${hivevar:scene} partition(date_id=${hivevar:proc_date});
