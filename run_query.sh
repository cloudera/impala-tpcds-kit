#!/bin/bash

dir_list="99query"
QUERY_DIR=queries

rm -rf logs
mkdir logs

for t in $(cat $dir_list)
do 
  echo "current query will be ${QUERY_DIR}/${t}"
    impala-shell -d tpcds_parquet -f ${QUERY_DIR}/${t} &>logs/${t}.log 
#   hive  -d tpcds_parquet -f ${QUERY_DIR}/${t} &>logs/${t}.log
done
echo "all queries execution are finished, please check logs for the result!"
