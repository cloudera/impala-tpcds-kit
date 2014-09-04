#!/bin/bash
source tpcds-env.sh

# find out what our node number is
source nodenum.sh

count=$DSDGEN_THREADS_PER_NODE

start=(NODENUM-1)*$count+1

TPCDS_ROOT="${CLUSTER_HOMEDIR}/${TPCDS_DIR}"

# hard coded for the store_sales table for now
t=store_sales

for (( c=$start; c<($count+$start); c++ ))
do
  echo "Generating part $c of ${DSDGEN_TOTAL_THREADS}"
  ${TPCDS_ROOT}/tools/dsdgen \
    -TABLE $t \
    -SCALE ${TPCDS_SCALE_FACTOR} \
    -CHILD $c \
    -PARALLEL ${DSDGEN_TOTAL_THREADS} \
    -DISTRIBUTIONS ${TPCDS_ROOT}/tools/tpcds.idx \
    -TERMINATE N \
    -FILTER Y \
    -QUIET Y | /opt/vertica/bin/vsql -U ${VERTICA_USER} -w ${VERTICA_PW} -h ${VERTICA_HOST} -d ${VERTICA_DB} -p ${VERTICA_PORT} -c "COPY ${t} FROM STDIN DELIMITER '|'" &
done
wait
