drop table if exists customer_cached;
create table customer_cached as select * from customer;
drop table if exists customer_address_cached;
create table customer_address_cached as select * from customer_address;
drop table if exists date_dim_cached;
create table date_dim_cached as select * from date_dim;
drop table if exists household_demographics_cached;
create table household_demographics_cached as select * from household_demographics;
drop table if exists item_cached;
create table item_cached as select * from item;
drop table if exists store_cached;
create table store_cached as select * from store;

drop table if exists store_sales_cached;
create table store_sales_cached as
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
  cast(ss_sold_date_sk as int)
  from store_sales
  where
  ss_sold_date_sk between 2451149 and 2451179 OR
  ss_sold_date_sk between 2451911 and 2452275 OR
  ss_sold_date_sk between 2451180 and 2451269 OR
  ss_sold_date_sk between 2451911 and 2451941 OR
  ss_sold_date_sk between 2451484 and 2451513
;
