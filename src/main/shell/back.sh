#!/bin/bash
source /opt/beh/conf/hdp_env
project_path=$(dirname $(dirname $(dirname $( cd $( dirname ${BASH_SOURCE[0]} ) && pwd ))))"/"
proc_date=$(date +%Y%m%d -d " ")
run_time=$(date "+%H%M")
scene="will"

to_run=${project_path}target/finance_marketing_distributed-1.0-SNAPSHOT-jar-with-dependencies.jar

spark-submit --master yarn \
             --deploy-mode cluster \
             --name machine_learning_xgb \
             --num-executors 40 \
             --executor-memory 32g \
             --driver-memory 32g \
             --executor-cores 16 \
             --conf spark.shuffle.service.enabled=true \
             --conf spark.dynamicAllocation.enabled=true \
             --conf spark.dynamicAllocation.minExecutors=4 \
             --conf spark.dynamicAllocation.maxExecutors=40 \
             --conf spark.shuffle.useOldFetchProtocol=true \
             --conf spark.executor.heartbeatInterval=1800s \
             --conf spark.network.timeout=2400s \
             --conf spark.driver.extraJavaOptions=" -Xss30M" \
      	     --conf spark.driver.extraJavaOptions=" -Xms30g -Xmn24g -Xss30M -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:MetaspaceSize=128M -Xloggc:/tmp/gc.log" \
	           --conf spark.executor.extraJavaOptions="-Xss30M -XX:+PrintGC -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:MetaspaceSize=128M -Dlog4j.configuration=log4j.properties" \
             --files log4j.properties \
             --class finance.marketing.distributed.proc \
             ${to_run}  \
             -feat1 "/tmp/finance/finance_marketing_feature.csv" \
             -feat1col "/tmp/finance/finance_marketing_feature_cols.csv" \
             -feat2 "/tmp/finance/finance_marketing_feature_recent_visit.csv" \
             -feat2col "/tmp/finance/finance_marketing_feature_recent_visit_cols.csv" \
             -labelPath "/tmp/finance/connect_will_label.csv" \
             -labelColPath "/tmp/finance/labelCol.csv" \
             -selectedFeat "" \
             -modelPath "/tmp/finance/feedback/${scene}_${proc_date}" \
             -procType "train" \
             -algVersion "alg_2C_V2" \
             -scene ${scene} \
             -predictionPath "/tmp/finance/feedback/prediction_${scene}_${proc_date}"
