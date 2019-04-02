#!/bin/bash


MASTER="local[2]"
NAME="pyspark-example"

path=$1

PY_FILE="dist/jobs.zip"
CMD="src/main.py --job findBestRecipe \
     --job-args path=${path}/mergeNginx profile=dev"

SPARK_SUBMIT=$(which spark-submit)

SUBMIT_COMMAND="$SPARK_SUBMIT \
                --master $MASTER \
                --deploy-mode client \
                --driver-memory 4g \
                --executor-memory 4g \
                --executor-cores 2 \
                --conf spark.app.name=$NAME \
                --conf spark.dynamicAllocation.enabled=true \
                --conf spark.network.timeout=36000 \
                --conf spark.sql.broadcastTimeout=7200 \
                --conf spark.executor.heartbeatInterval=60 \
                --conf spark.rdd.compress=true \
                --conf spark.driver.maxResultSize=8g \
                --conf spark.rpc.message.maxSize=256 \
                --conf "spark.driver.extraJavaOptions=-Dfile.encoding=utf-8" \
                --py-files $PY_FILE \
                $CMD
                "


echo "$SUBMIT_COMMAND"

eval "$SUBMIT_COMMAND"