#!/bin/bash
set -ex

source tpcds-env.sh

python vertica/run_queries.py \
  --vertica-username ${VERTICA_USER} \
  --vertica-password ${VERTICA_PW} \
  --vertica-database ${VERTICA_DB} \
  --vertica-host ${VERTICA_HOST} \
  "$@"
