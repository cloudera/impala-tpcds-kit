#!/bin/bash
source tpcds-env.sh

TPCDS_ROOT="${CLUSTER_HOMEDIR}/${TPCDS_DIR}"

for t in date_dim time_dim customer customer_address customer_demographics household_demographics item promotion store
do
  echo "Generating table $t"
  ${TPCDS_ROOT}/tools/dsdgen \
    -TABLE $t \
    -SCALE ${TPCDS_SCALE_FACTOR} \
    -DISTRIBUTIONS ${TPCDS_ROOT}/tools/tpcds.idx \
    -TERMINATE N \
    -FILTER Y \
    -QUIET Y | /opt/vertica/bin/vsql -U ${VERTICA_USER} -w ${VERTICA_PW} -h ${VERTICA_HOST} -d ${VERTICA_DB} -p ${VERTICA_PORT} -c "COPY ${t} FROM STDIN DELIMITER '|'" &

done
wait
