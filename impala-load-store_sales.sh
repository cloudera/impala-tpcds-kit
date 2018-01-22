#!/bin/bash
source tpcds-env.sh

impala-shell $IMPALA_SHELL_OPTS -d ${TPCDS_DBNAME} -q "
set REQUEST_POOL=$REQUEST_POOL;
drop table if exists store_sales;

create table store_sales
(
  ss_sold_time_sk bigint,
  ss_item_sk bigint,
  ss_customer_sk bigint,
  ss_cdemo_sk bigint,
  ss_hdemo_sk bigint,
  ss_addr_sk bigint,
  ss_store_sk bigint,
  ss_promo_sk bigint,
  ss_ticket_number bigint,
  ss_quantity int,
  ss_wholesale_cost decimal(7,2),
  ss_list_price decimal(7,2),
  ss_sales_price decimal(7,2),
  ss_ext_discount_amt decimal(7,2),
  ss_ext_sales_price decimal(7,2),
  ss_ext_wholesale_cost decimal(7,2),
  ss_ext_list_price decimal(7,2),
  ss_ext_tax decimal(7,2),
  ss_coupon_amt decimal(7,2),
  ss_net_paid decimal(7,2),
  ss_net_paid_inc_tax decimal(7,2),
  ss_net_profit decimal(7,2)
)
partitioned by (ss_sold_date_sk bigint)
stored as parquetfile"

python load-store-sales.py
