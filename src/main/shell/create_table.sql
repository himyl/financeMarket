                                                                                --loan
set mapreduce.job.queuename=xl_non_production;
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


drop table if exists XXX;
CREATE TABLE if not exists XXX(
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

drop table if exists XXX;
CREATE TABLE if not exists XXX(
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

drop table if exists XXX;
CREATE TABLE if not exists XXX(
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

drop table if exists XXX;
CREATE TABLE if not exists XXX(
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
