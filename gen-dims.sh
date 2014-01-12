#!/bin/bash
source tpcds-env.sh

for t in date_dim time_dim customer customer_address customer_demographics household_demographics item promotion store
do
  echo "Generating table $t"
  ${TPCDS_ROOT}/tools/dsdgen \
    -TABLE $t \
    -SCALE ${TPCDS_SCALE_FACTOR} \
    -DISTRIBUTIONS ${TPCDS_ROOT}/tools/tpcds.idx \
    -TERMINATE N \
    -FILTER Y \
    -QUIET Y | hdfs dfs -put - ${FLATFILE_HDFS_ROOT}/${t}/${t}.dat &
done
wait

hdfs dfs -ls -R ${FLATFILE_HDFS_ROOT}/*/*.dat
