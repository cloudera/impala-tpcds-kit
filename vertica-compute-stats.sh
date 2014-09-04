#!/bin/bash
source tpcds-env.sh

ssh $CLUSTER_USER@$h "/opt/vertica/bin/vsql -U ${VERTICA_USER} -w ${VERTICA_PW} -h ${VERTICA_HOST} -d ${VERTICA_DB} -p ${VERTICA_PORT} -c "select audit('', 'table'); select object_name, size_bytes, cell_count from v_catalog.user_audits"
