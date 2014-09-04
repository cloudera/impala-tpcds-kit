#!/bin/bash
source tpcds-env.sh

h=`head -n 1 dn.txt`

QUERY=$(cat <<EOF
SELECT anchor_table_schema,
       anchor_table_name,
       ROUND(SUM(used_bytes) / ( 1024^3 ), 3) AS used_compressed_gb
FROM   v_monitor.projection_storage
GROUP  BY anchor_table_schema,
          anchor_table_name
ORDER  BY SUM(used_bytes) DESC;
EOF
)

ssh $CLUSTER_USER@$h "/opt/vertica/bin/vsql -U ${VERTICA_USER} -w ${VERTICA_PW} -h ${VERTICA_HOST} -d ${VERTICA_DB} -p ${VERTICA_PORT} -c \"$QUERY\""
