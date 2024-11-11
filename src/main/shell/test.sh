#!/bin/bash
source /opt/beh/conf/hdp_env
project_path=$(dirname $(dirname $(dirname $( cd $( dirname ${BASH_SOURCE[0]} ) && pwd ))))"/"
project_name="test"


to_run=${project_path}target/finance_marketing_distributed-1.0-SNAPSHOT-jar-with-dependencies.jar

spark-submit --master yarn \
             --deploy-mode client \
             --num-executors 20 \
             --executor-memory 8g \
             --executor-cores 4 \
             --class finance.marketing.distributed.proc_test \
             ${to_run}  \
             "/tmp/limy/finance_marketing_feature_w.csv" \
             "/tmp/limy/finance_marketing_feature.csv" \
             "/tmp/limy/finance_marketing_feature_cols.csv"
