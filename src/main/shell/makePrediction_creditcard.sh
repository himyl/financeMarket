#!/bin/bash
source /opt/beh/conf/hdp_env
project_path=$(dirname $(dirname $(dirname $( cd $( dirname ${BASH_SOURCE[0]} ) && pwd ))))"/"
proc_date=$1
product="creditcard"
scene="will"

to_run=${project_path}target/finance_marketing_distributed-1.0-SNAPSHOT-jar-with-dependencies.jar
spark-submit --master yarn \
             --deploy-mode client \
             --name inference_${product}_${scene} \
             --num-executors 20 \
             --executor-memory 32g \
             --driver-memory 64g \
             --executor-cores 8 \
             --conf spark.port.maxRetries=300 \
             --conf spark.executor.heartbeatInterval=54000s \
             --conf spark.network.timeout=72000s \
             --conf spark.driver.extraJavaOptions=" -Xss30M" \
      	     --conf spark.driver.extraJavaOptions=" -Xms30g -Xmn24g -Xss30M -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:MetaspaceSize=128M -Xloggc:/tmp/gc.log" \
	           --conf spark.executor.extraJavaOptions="-Xss30M -XX:+PrintGC -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:MetaspaceSize=128M -Dlog4j.configuration=log4j.properties" \
             --files log4j.properties \
             --class finance.marketing.distributed.proc \
             ${to_run}  \
             -feat1 "XXX/${proc_date}/finance_marketing_feature.csv" \
             -feat1col "XXX/finance_marketing_feature_cols.csv" \
             -feat2 "XXX/${proc_date}/finance_marketing_feature_recent_visit.csv" \
             -feat2col "XXX/finance_marketing_feature_recent_visit_cols.csv" \
             -selectedFeat "" \
             -modelPath "XXX/${scene}_XXX" \
             -procType "predict" \
             -algVersion "alg_2C_V1" \
             -scene ${scene} \
             -predictionPath "XXX/${product}_${scene}_${proc_date}"
echo "creditcard will inference succeed"

scene="connect"
to_run=${project_path}target/finance_marketing_distributed-1.0-SNAPSHOT-jar-with-dependencies.jar
spark-submit --master yarn \
             --deploy-mode client \
             --name inference_${product}_${scene} \
             --num-executors 20 \
             --executor-memory 32g \
             --driver-memory 64g \
             --executor-cores 8 \
             --conf spark.executor.heartbeatInterval=54000s \
             --conf spark.network.timeout=72000s \
             --conf spark.driver.extraJavaOptions=" -Xss30M" \
      	     --conf spark.driver.extraJavaOptions=" -Xms30g -Xmn24g -Xss30M -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:MetaspaceSize=128M -Xloggc:/tmp/gc.log" \
	           --conf spark.executor.extraJavaOptions="-Xss30M -XX:+PrintGC -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:MetaspaceSize=128M -Dlog4j.configuration=log4j.properties" \
             --files log4j.properties \
             --class finance.marketing.distributed.proc \
             ${to_run}  \
             -feat1 "XXX/${proc_date}/finance_marketing_feature.csv" \
             -feat1col "XXX/finance_marketing_feature_cols.csv" \
             -feat2 "XXX/${proc_date}/finance_marketing_feature_recent_visit.csv" \
             -feat2col "XXX/finance_marketing_feature_recent_visit_cols.csv" \
             -selectedFeat "" \
             -modelPath "/XXX/${scene}_XXX" \
             -procType "predict" \
             -algVersion "alg_2C_V1" \
             -scene ${scene} \
             -predictionPath "XXX/${product}_${scene}_${proc_date}"

echo "creditcard connect inference succeed"
