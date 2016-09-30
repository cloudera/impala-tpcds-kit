#!/bin/bash
source ./tpcds-env.sh

impala-shell -d ${TPCDS_DBNAME} -q "
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
stored as parquetfile;


drop table if exists store_returns;
create table store_returns
(
    sr_return_time_sk         bigint                       ,
    sr_item_sk                bigint               ,
    sr_customer_sk            bigint                       ,
    sr_cdemo_sk               bigint                       ,
    sr_hdemo_sk               bigint                       ,
    sr_addr_sk                bigint                       ,
    sr_store_sk               bigint                       ,
    sr_reason_sk              bigint                       ,
    sr_ticket_number          bigint               ,
    sr_return_quantity        bigint                       ,
    sr_return_amt             double                  ,
    sr_return_tax             double                  ,
    sr_return_amt_inc_tax     double                  ,
    sr_fee                    double                  ,
    sr_return_ship_cost       double                  ,
    sr_refunded_cash          double                  ,
    sr_reversed_charge        double                  ,
    sr_store_credit           double                  ,
    sr_net_loss               double                  
)
partitioned by (sr_returned_date_sk bigint)
stored as parquetfile;

drop table if exists web_sales;
create table web_sales
(
    ws_sold_time_sk           bigint                       ,
    ws_ship_date_sk           bigint                       ,
    ws_item_sk                bigint               ,
    ws_bill_customer_sk       bigint                       ,
    ws_bill_cdemo_sk          bigint                       ,
    ws_bill_hdemo_sk          bigint                       ,
    ws_bill_addr_sk           bigint                       ,
    ws_ship_customer_sk       bigint                       ,
    ws_ship_cdemo_sk          bigint                       ,
    ws_ship_hdemo_sk          bigint                       ,
    ws_ship_addr_sk           bigint                       ,
    ws_web_page_sk            bigint                       ,
    ws_web_site_sk            bigint                       ,
    ws_ship_mode_sk           bigint                       ,
    ws_warehouse_sk           bigint                       ,
    ws_promo_sk               bigint                       ,
    ws_order_number           bigint               ,
    ws_quantity               bigint                       ,
    ws_wholesale_cost         double                  ,
    ws_list_price             double                  ,
    ws_sales_price            double                  ,
    ws_ext_discount_amt       double                  ,
    ws_ext_sales_price        double                  ,
    ws_ext_wholesale_cost     double                  ,
    ws_ext_list_price         double                  ,
    ws_ext_tax                double                  ,
    ws_coupon_amt             double                  ,
    ws_ext_ship_cost          double                  ,
    ws_net_paid               double                  ,
    ws_net_paid_inc_tax       double                  ,
    ws_net_paid_inc_ship      double                  ,
    ws_net_paid_inc_ship_tax  double                  ,
    ws_net_profit             double                          
)
partitioned by (ws_sold_date_sk bigint)
stored as parquetfile;


drop table if exists web_returns;
create table web_returns
(
    wr_returned_time_sk       bigint                       ,
    wr_item_sk                bigint               ,
    wr_refunded_customer_sk   bigint                       ,
    wr_refunded_cdemo_sk      bigint                       ,
    wr_refunded_hdemo_sk      bigint                       ,
    wr_refunded_addr_sk       bigint                       ,
    wr_returning_customer_sk  bigint                       ,
    wr_returning_cdemo_sk     bigint                       ,
    wr_returning_hdemo_sk     bigint                       ,
    wr_returning_addr_sk      bigint                       ,
    wr_web_page_sk            bigint                       ,
    wr_reason_sk              bigint                       ,
    wr_order_number           bigint               ,
    wr_return_quantity        bigint                       ,
    wr_return_amt             double                  ,
    wr_return_tax             double                  ,
    wr_return_amt_inc_tax     double                  ,
    wr_fee                    double                  ,
    wr_return_ship_cost       double                  ,
    wr_refunded_cash          double                  ,
    wr_reversed_charge        double                  ,
    wr_account_credit         double                  ,
    wr_net_loss               double                  
     
)
partitioned by (wr_returned_date_sk bigint)
stored as parquetfile;

drop table if exists catalog_sales;
create table catalog_sales
(
    cs_sold_time_sk           bigint                       ,
    cs_ship_date_sk           bigint                       ,
    cs_bill_customer_sk       bigint                       ,
    cs_bill_cdemo_sk          bigint                       ,
    cs_bill_hdemo_sk          bigint                       ,
    cs_bill_addr_sk           bigint                       ,
    cs_ship_customer_sk       bigint                       ,
    cs_ship_cdemo_sk          bigint                       ,
    cs_ship_hdemo_sk          bigint                       ,
    cs_ship_addr_sk           bigint                       ,
    cs_call_center_sk         bigint                       ,
    cs_catalog_page_sk        bigint                       ,
    cs_ship_mode_sk           bigint                       ,
    cs_warehouse_sk           bigint                       ,
    cs_item_sk                bigint               ,
    cs_promo_sk               bigint                       ,
    cs_order_number           bigint               ,
    cs_quantity               bigint                       ,
    cs_wholesale_cost         double                  ,
    cs_list_price             double                  ,
    cs_sales_price            double                  ,
    cs_ext_discount_amt       double                  ,
    cs_ext_sales_price        double                  ,
    cs_ext_wholesale_cost     double                  ,
    cs_ext_list_price         double                  ,
    cs_ext_tax                double                  ,
    cs_coupon_amt             double                  ,
    cs_ext_ship_cost          double                  ,
    cs_net_paid               double                  ,
    cs_net_paid_inc_tax       double                  ,
    cs_net_paid_inc_ship      double                  ,
    cs_net_paid_inc_ship_tax  double                  ,
    cs_net_profit             double                  
)
partitioned by (cs_sold_date_sk bigint)
stored as parquetfile;


drop table if exists catalog_returns;
create table catalog_returns
(
    cr_returned_time_sk       bigint                       ,
    cr_item_sk                bigint               ,
    cr_refunded_customer_sk   bigint                       ,
    cr_refunded_cdemo_sk      bigint                       ,
    cr_refunded_hdemo_sk      bigint                       ,
    cr_refunded_addr_sk       bigint                       ,
    cr_returning_customer_sk  bigint                       ,
    cr_returning_cdemo_sk     bigint                       ,
    cr_returning_hdemo_sk     bigint                       ,
    cr_returning_addr_sk      bigint                       ,
    cr_call_center_sk         bigint                       ,
    cr_catalog_page_sk        bigint                       ,
    cr_ship_mode_sk           bigint                       ,
    cr_warehouse_sk           bigint                       ,
    cr_reason_sk              bigint                       ,
    cr_order_number           bigint               ,
    cr_return_quantity        bigint                       ,
    cr_return_amount          double                  ,
    cr_return_tax             double                  ,
    cr_return_amt_inc_tax     double                  ,
    cr_fee                    double                  ,
    cr_return_ship_cost       double                  ,
    cr_refunded_cash          double                  ,
    cr_reversed_charge        double                  ,
    cr_store_credit           double                  ,
    cr_net_loss               double                  
)
partitioned by (cr_returned_date_sk bigint)
stored as parquetfile;

drop table if exists inventory;
create table inventory
(
  inv_item_sk               bigint,
  inv_warehouse_sk          bigint,
  inv_quantity_on_hand      int
)
partitioned by (inv_date_sk bigint)
stored as parquetfile;



"

python load-store-sales.py &
python load-store-returns.py &
python load-web-sales.py &
python load-web-returns.py &
python load-catalog-sales.py &
python load-catalog-returns.py &
python load-inventory.py &







