#!/bin/bash
source tpcds-env.sh

impala-shell $IMPALA_SHELL_OPTS -d $TPCDS_DBNAME <<EOF
set REQUEST_POOL=$REQUEST_POOL;
compute stats date_dim;
compute stats time_dim;
compute stats customer;
compute stats customer_address;
compute stats customer_demographics;
compute stats household_demographics;
compute stats item;
compute stats promotion;
compute stats store;
compute stats store_sales;
EOF
