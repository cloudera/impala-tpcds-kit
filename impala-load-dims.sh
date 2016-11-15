#!/bin/bash
source ./tpcds-env.sh

impala-shell -d $TPCDS_DBNAME <<EOF
create table date_dim like et_date_dim stored as parquetfile;
insert overwrite table date_dim select * from et_date_dim;

create table time_dim like et_time_dim stored as parquetfile;
insert overwrite table time_dim select * from et_time_dim;

create table customer like et_customer stored as parquetfile;
insert overwrite table customer select * from et_customer;

create table customer_address like et_customer_address stored as parquetfile;
insert overwrite table customer_address select * from et_customer_address;

create table customer_demographics like et_customer_demographics stored as parquetfile;
insert overwrite table customer_demographics select * from et_customer_demographics;

create table household_demographics like et_household_demographics stored as parquetfile;
insert overwrite table household_demographics select * from et_household_demographics;

create table item like et_item stored as parquetfile;
insert overwrite table item select * from et_item;

create table promotion like et_promotion stored as parquetfile;
insert overwrite table promotion select * from et_promotion;

create table store like et_store stored as parquetfile;
insert overwrite table store select * from et_store;

create table warehouse like et_warehouse stored as parquetfile;
insert overwrite table warehouse select * from et_warehouse;

create table ship_mode like et_ship_mode stored as parquetfile;
insert overwrite table ship_mode select * from et_ship_mode;

create table reason like et_reason stored as parquetfile;
insert overwrite table reason select * from et_reason;

create table income_band like et_income_band stored as parquetfile;
insert overwrite table income_band select * from et_income_band;

create table call_center like et_call_center stored as parquetfile;
insert overwrite table call_center select * from et_call_center;

create table web_page like et_web_page stored as parquetfile;
insert overwrite table web_page select * from et_web_page;

create table catalog_page like et_catalog_page stored as parquetfile;
insert overwrite table catalog_page select * from et_catalog_page;

create table web_site like et_web_site stored as parquetfile;
insert overwrite table web_site select * from et_web_site;




show tables;
EOF
