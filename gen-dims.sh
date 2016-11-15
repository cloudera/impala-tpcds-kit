#!/bin/bash
source ./tpcds-env.sh

echo ${HOME}

for t in $dims
do
  echo "Generating table $t"
  ${TPCDS_ROOT}/tools/dsdgen \
    -TABLE $t \
    -SCALE ${TPCDS_SCALE_FACTOR} \
    -DISTRIBUTIONS ${TPCDS_ROOT}/tools/tpcds.idx \
    -TERMINATE N \
    -_FILTER Y \
    -QUIET Y \
    -DIR ${HOME} \
    -FORCE Y

    hadoop fs -put ${HOME}/${t}.dat ${FLATFILE_HDFS_ROOT}/${t}/${t}.dat &
done
wait

