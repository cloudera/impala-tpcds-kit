#!/bin/bash
set -ex

source tpcds-env.sh

python vertica/create_tables.py \
  --vertica-username ${VERTICA_USER} \
  --vertica-password ${VERTICA_PW} \
  --vertica-database ${VERTICA_DB} \
  --vertica-host ${VERTICA_HOST} \
  --delete
