--
-- adjust the schema name if necessary
-- currently (tpcds_10000_parquet)
--

create schema tpcds_10000_parquet;
use tpcds_10000_parquet;

--
-- unpartitioned tables
--
create table call_center            like tpcds_10000_text.call_center            stored as parquet;
create table catalog_page           like tpcds_10000_text.catalog_page           stored as parquet;
create table customer               like tpcds_10000_text.customer               stored as parquet;
create table customer_address       like tpcds_10000_text.customer_address       stored as parquet;
create table customer_demographics  like tpcds_10000_text.customer_demographics  stored as parquet;
create table date_dim               like tpcds_10000_text.date_dim               stored as parquet;
create table household_demographics like tpcds_10000_text.household_demographics stored as parquet;
create table income_band            like tpcds_10000_text.income_band            stored as parquet;
create table item                   like tpcds_10000_text.item                   stored as parquet;
create table promotion              like tpcds_10000_text.promotion              stored as parquet;
create table reason                 like tpcds_10000_text.reason                 stored as parquet;
create table ship_mode              like tpcds_10000_text.ship_mode              stored as parquet;
create table store                  like tpcds_10000_text.store                  stored as parquet;
create table time_dim               like tpcds_10000_text.time_dim               stored as parquet;
create table warehouse              like tpcds_10000_text.warehouse              stored as parquet;
create table web_page               like tpcds_10000_text.web_page               stored as parquet;
create table web_site               like tpcds_10000_text.web_site               stored as parquet;

--
-- partitioned tables
--

create table inventory
(
  inv_item_sk                 integer,
  inv_warehouse_sk            integer,
  inv_quantity_on_hand        integer
)
partitioned by (inv_date_sk integer)
stored as parquet;

create table store_sales
(
  ss_sold_time_sk             integer,
  ss_item_sk                  integer,
  ss_customer_sk              integer,
  ss_cdemo_sk                 integer,
  ss_hdemo_sk                 integer,
  ss_addr_sk                  integer,
  ss_store_sk                 integer,
  ss_promo_sk                 integer,
  ss_ticket_number            bigint,
  ss_quantity                 integer,
  ss_wholesale_cost           decimal(7,2),
  ss_list_price               decimal(7,2),
  ss_sales_price              decimal(7,2),
  ss_ext_discount_amt         decimal(7,2),
  ss_ext_sales_price          decimal(7,2),
  ss_ext_wholesale_cost       decimal(7,2),
  ss_ext_list_price           decimal(7,2),
  ss_ext_tax                  decimal(7,2),
  ss_coupon_amt               decimal(7,2),
  ss_net_paid                 decimal(7,2),
  ss_net_paid_inc_tax         decimal(7,2),
  ss_net_profit               decimal(7,2)
)
partitioned by (ss_sold_date_sk integer)
stored as parquet;

create table store_returns
(
  sr_return_time_sk         integer,
  sr_item_sk                integer,
  sr_customer_sk            integer,
  sr_cdemo_sk               integer,
  sr_hdemo_sk               integer,
  sr_addr_sk                integer,
  sr_store_sk               integer,
  sr_reason_sk              integer,
  sr_ticket_number          bigint,
  sr_return_quantity        integer,
  sr_return_amt             decimal(7,2),
  sr_return_tax             decimal(7,2),
  sr_return_amt_inc_tax     decimal(7,2),
  sr_fee                    decimal(7,2),
  sr_return_ship_cost       decimal(7,2),
  sr_refunded_cash          decimal(7,2),
  sr_reversed_charge        decimal(7,2),
  sr_store_credit           decimal(7,2),
  sr_net_loss               decimal(7,2)
)
partitioned by (sr_returned_date_sk integer)
stored as parquet;

create table catalog_returns
(
  cr_returned_time_sk       integer,
  cr_item_sk                integer,
  cr_refunded_customer_sk   integer,
  cr_refunded_cdemo_sk      integer,
  cr_refunded_hdemo_sk      integer,
  cr_refunded_addr_sk       integer,
  cr_returning_customer_sk  integer,
  cr_returning_cdemo_sk     integer,
  cr_returning_hdemo_sk     integer,
  cr_returning_addr_sk      integer,
  cr_call_center_sk         integer,
  cr_catalog_page_sk        integer,
  cr_ship_mode_sk           integer,
  cr_warehouse_sk           integer,
  cr_reason_sk              integer,
  cr_order_number           bigint,
  cr_return_quantity        integer,
  cr_return_amount          decimal(7,2),
  cr_return_tax             decimal(7,2),
  cr_return_amt_inc_tax     decimal(7,2),
  cr_fee                    decimal(7,2),
  cr_return_ship_cost       decimal(7,2),
  cr_refunded_cash          decimal(7,2),
  cr_reversed_charge        decimal(7,2),
  cr_store_credit           decimal(7,2),
  cr_net_loss               decimal(7,2)
)
partitioned by (cr_returned_date_sk integer)
stored as parquet;

create table catalog_sales
(
  cs_sold_time_sk           integer,
  cs_ship_date_sk           integer,
  cs_bill_customer_sk       integer,
  cs_bill_cdemo_sk          integer,
  cs_bill_hdemo_sk          integer,
  cs_bill_addr_sk           integer,
  cs_ship_customer_sk       integer,
  cs_ship_cdemo_sk          integer,
  cs_ship_hdemo_sk          integer,
  cs_ship_addr_sk           integer,
  cs_call_center_sk         integer,
  cs_catalog_page_sk        integer,
  cs_ship_mode_sk           integer,
  cs_warehouse_sk           integer,
  cs_item_sk                integer,
  cs_promo_sk               integer,
  cs_order_number           bigint,
  cs_quantity               integer,
  cs_wholesale_cost         decimal(7,2),
  cs_list_price             decimal(7,2),
  cs_sales_price            decimal(7,2),
  cs_ext_discount_amt       decimal(7,2),
  cs_ext_sales_price        decimal(7,2),
  cs_ext_wholesale_cost     decimal(7,2),
  cs_ext_list_price         decimal(7,2),
  cs_ext_tax                decimal(7,2),
  cs_coupon_amt             decimal(7,2),
  cs_ext_ship_cost          decimal(7,2),
  cs_net_paid               decimal(7,2),
  cs_net_paid_inc_tax       decimal(7,2),
  cs_net_paid_inc_ship      decimal(7,2),
  cs_net_paid_inc_ship_tax  decimal(7,2),
  cs_net_profit             decimal(7,2)
)
partitioned by (cs_sold_date_sk bigint)
stored as parquet;

create table web_returns
(
  wr_returned_time_sk       integer,
  wr_item_sk                integer,
  wr_refunded_customer_sk   integer,
  wr_refunded_cdemo_sk      integer,
  wr_refunded_hdemo_sk      integer,
  wr_refunded_addr_sk       integer,
  wr_returning_customer_sk  integer,
  wr_returning_cdemo_sk     integer,
  wr_returning_hdemo_sk     integer,
  wr_returning_addr_sk      integer,
  wr_web_page_sk            integer,
  wr_reason_sk              integer,
  wr_order_number           bigint,
  wr_return_quantity        integer,
  wr_return_amt             decimal(7,2),
  wr_return_tax             decimal(7,2),
  wr_return_amt_inc_tax     decimal(7,2),
  wr_fee                    decimal(7,2),
  wr_return_ship_cost       decimal(7,2),
  wr_refunded_cash          decimal(7,2),
  wr_reversed_charge        decimal(7,2),
  wr_account_credit         decimal(7,2),
  wr_net_loss               decimal(7,2)
)
partitioned by (wr_returned_date_sk integer)
stored as parquet;

create table web_sales
(
  ws_sold_time_sk           integer,
  ws_ship_date_sk           integer,
  ws_item_sk                integer,
  ws_bill_customer_sk       integer,
  ws_bill_cdemo_sk          integer,
  ws_bill_hdemo_sk          integer,
  ws_bill_addr_sk           integer,
  ws_ship_customer_sk       integer,
  ws_ship_cdemo_sk          integer,
  ws_ship_hdemo_sk          integer,
  ws_ship_addr_sk           integer,
  ws_web_page_sk            integer,
  ws_web_site_sk            integer,
  ws_ship_mode_sk           integer,
  ws_warehouse_sk           integer,
  ws_promo_sk               integer,
  ws_order_number           bigint,
  ws_quantity               integer,
  ws_wholesale_cost         decimal(7,2),
  ws_list_price             decimal(7,2),
  ws_sales_price            decimal(7,2),
  ws_ext_discount_amt       decimal(7,2),
  ws_ext_sales_price        decimal(7,2),
  ws_ext_wholesale_cost     decimal(7,2),
  ws_ext_list_price         decimal(7,2),
  ws_ext_tax                decimal(7,2),
  ws_coupon_amt             decimal(7,2),
  ws_ext_ship_cost          decimal(7,2),
  ws_net_paid               decimal(7,2),
  ws_net_paid_inc_tax       decimal(7,2),
  ws_net_paid_inc_ship      decimal(7,2),
  ws_net_paid_inc_ship_tax  decimal(7,2),
  ws_net_profit             decimal(7,2)
)
partitioned by (ws_sold_date_sk integer)
stored as parquet;
