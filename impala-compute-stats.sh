#!/bin/bash
source tpcds-env.sh

impala-shell -d $TPCDS_DBNAME <<EOF
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
