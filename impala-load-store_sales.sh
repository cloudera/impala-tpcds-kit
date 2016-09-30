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

insert overwrite table catalog_returns
              partition(cr_returned_date_sk) [shuffle]
              select
    cr_returned_time_sk,
    cr_item_sk,
    cr_refunded_customer_sk ,
    cr_refunded_cdemo_sk,
    cr_refunded_hdemo_sk,
    cr_refunded_addr_sk,
    cr_returning_customer_sk,
    cr_returning_cdemo_sk,
    cr_returning_hdemo_sk ,
    cr_returning_addr_sk ,
    cr_call_center_sk,
    cr_catalog_page_sk,
    cr_ship_mode_sk,
    cr_warehouse_sk,
    cr_reason_sk,
    cr_order_number,
    cr_return_quantity,
    cr_return_amount,
    cr_return_tax ,
    cr_return_amt_inc_tax,
    cr_fee  ,
    cr_return_ship_cost ,
    cr_refunded_cash,
    cr_reversed_charge,
    cr_store_credit,
    cr_net_loss,
    cr_returned_date_sk
from et_catalog_returns;

insert overwrite table catalog_sales
              partition(cs_sold_date_sk) [shuffle]
              select
              cs_sold_time_sk ,
    cs_ship_date_sk   ,
    cs_bill_customer_sk ,
    cs_bill_cdemo_sk  ,
    cs_bill_hdemo_sk  ,
    cs_bill_addr_sk   ,
    cs_ship_customer_sk ,
    cs_ship_cdemo_sk ,
    cs_ship_hdemo_sk ,
    cs_ship_addr_sk ,
    cs_call_center_sk,
    cs_catalog_page_sk ,
    cs_ship_mode_sk ,
    cs_warehouse_sk,
    cs_item_sk  ,
    cs_promo_sk   ,
    cs_order_number  ,
    cs_quantity ,
    cs_wholesale_cost,
    cs_list_price  ,
    cs_sales_price  ,
    cs_ext_discount_amt ,
    cs_ext_sales_price  ,
    cs_ext_wholesale_cost ,
    cs_ext_list_price  ,
    cs_ext_tax  ,
    cs_coupon_amt,
    cs_ext_ship_cost ,
    cs_net_paid,
    cs_net_paid_inc_tax,
    cs_net_paid_inc_ship,
    cs_net_paid_inc_ship_tax,
    cs_net_profit,
    cs_sold_date_sk
from et_catalog_sales;

insert overwrite table inventory
              partition(inv_date_sk) [shuffle]
              select
  inv_item_sk,
  inv_warehouse_sk,
  inv_quantity_on_hand,
  inv_date_sk
              from et_inventory;
              
insert overwrite table store_returns
              partition(sr_returned_date_sk) [shuffle]
              select
    sr_return_time_sk ,
    sr_item_sk  ,
    sr_customer_sk ,
    sr_cdemo_sk  ,
    sr_hdemo_sk  ,
    sr_addr_sk ,
    sr_store_sk ,
    sr_reason_sk  ,
    sr_ticket_number  ,
    sr_return_quantity ,
    sr_return_amt,
    sr_return_tax  ,
    sr_return_amt_inc_tax,
    sr_fee ,
    sr_return_ship_cost,
    sr_refunded_cash   ,
    sr_reversed_charge  ,
    sr_store_credit,
    sr_net_loss,
    sr_returned_date_sk                
              from et_store_returns;
              
insert overwrite table store_sales
              partition(ss_sold_date_sk) [shuffle]
              select
              ss_sold_time_sk,
              ss_item_sk,
              ss_customer_sk,
              ss_cdemo_sk,
              ss_hdemo_sk,
              ss_addr_sk,
              ss_store_sk,
              ss_promo_sk,
              ss_ticket_number,
              ss_quantity,
              ss_wholesale_cost,
              ss_list_price,
              ss_sales_price,
              ss_ext_discount_amt,
              ss_ext_sales_price,
              ss_ext_wholesale_cost,
              ss_ext_list_price,
              ss_ext_tax,
              ss_coupon_amt,
              ss_net_paid,
              ss_net_paid_inc_tax,
              ss_net_profit,
              ss_sold_date_sk
              from et_store_sales;
              
insert overwrite table web_returns
              partition(wr_returned_date_sk) [shuffle]
              select
              wr_returned_time_sk,
    wr_item_sk  ,
    wr_refunded_customer_sk ,
    wr_refunded_cdemo_sk  ,
    wr_refunded_hdemo_sk  ,
    wr_refunded_addr_sk   ,
    wr_returning_customer_sk ,
    wr_returning_cdemo_sk  ,
    wr_returning_hdemo_sk  ,
    wr_returning_addr_sk  ,
    wr_web_page_sk ,
    wr_reason_sk ,
    wr_order_number,
    wr_return_quantity ,
    wr_return_amt  ,
    wr_return_tax   ,
    wr_return_amt_inc_tax,
    wr_fee,
    wr_return_ship_cost,
    wr_refunded_cash,
    wr_reversed_charge,
    wr_account_credit ,
    wr_net_loss,
    wr_returned_date_sk 
              from et_web_returns;
              
insert overwrite table web_sales
              partition(ws_sold_date_sk) [shuffle]
              select
              ws_sold_time_sk,
    ws_ship_date_sk,
    ws_item_sk,
    ws_bill_customer_sk,
    ws_bill_cdemo_sk,
    ws_bill_hdemo_sk,
    ws_bill_addr_sk ,
    ws_ship_customer_sk,
    ws_ship_cdemo_sk,
    ws_ship_hdemo_sk,
    ws_ship_addr_sk,
    ws_web_page_sk,
    ws_web_site_sk ,
    ws_ship_mode_sk ,
    ws_warehouse_sk    ,
    ws_promo_sk ,
    ws_order_number ,
    ws_quantity ,
    ws_wholesale_cost ,
    ws_list_price     ,
    ws_sales_price   ,
    ws_ext_discount_amt,
    ws_ext_sales_price ,
    ws_ext_wholesale_cost   ,
    ws_ext_list_price    ,
    ws_ext_tax ,
    ws_coupon_amt ,
    ws_ext_ship_cost ,
    ws_net_paid     ,
    ws_net_paid_inc_tax ,
    ws_net_paid_inc_ship ,
    ws_net_paid_inc_ship_tax ,
    ws_net_profit,
    ws_sold_date_sk                   
              from et_web_sales;

"

#python load-store-sales.py &
#python load-store-returns.py &
#python load-web-sales.py &
#python load-web-returns.py &
#python load-catalog-sales.py &
#python load-catalog-returns.py &
#python load-inventory.py &







