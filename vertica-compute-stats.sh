#!/bin/bash
source tpcds-env.sh

h=`head -n 1 dn.txt`

QUERY=$(cat <<EOF
select audit('', 'table');
select
    a1.object_name, round(a1.size_bytes/(1024^3), 2) as size_gb, a1.cell_count
from
    v_catalog.user_audits a1
join (
    select object_name, max(audit_start_timestamp) as audit_start_timestamp
    from v_catalog.user_audits
    group by object_name
) a2
on
    a1.object_name = a2.object_name
    and a1.audit_start_timestamp = a2.audit_start_timestamp;
EOF
)

ssh $CLUSTER_USER@$h "/opt/vertica/bin/vsql -U ${VERTICA_USER} -w ${VERTICA_PW} -h ${VERTICA_HOST} -d ${VERTICA_DB} -p ${VERTICA_PORT} -c \"$QUERY\""
