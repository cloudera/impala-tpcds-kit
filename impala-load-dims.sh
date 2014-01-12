#!/bin/bash
source tpcds-env.sh

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

show tables;
EOF
