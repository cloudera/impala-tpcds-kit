-- start query 1 in stream 0 using template query79.tpl
select
  c_last_name,
  c_first_name,
  substr(s_city, 1, 30) as city,
  ss_ticket_number,
  amt,
  profit
from
  (select
    /*+ MAPJOIN(household_demographics, date_dim, store) */
    ss_ticket_number,
    ss_customer_sk,
    s_city,
    sum(ss_coupon_amt) amt,
    sum(ss_net_profit) profit
  from
    store_sales
    join household_demographics on (store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk)
    join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
    join store on (store_sales.ss_store_sk = store.s_store_sk)
  where
    store.s_number_employees between 200 and 295
    and (household_demographics.hd_dep_count = 8
      or household_demographics.hd_vehicle_count > 0)
    and date_dim.d_dow = 1
    and date_dim.d_year in (1998, 1998 + 1, 1998 + 2)
    -- and ss_date between '1998-01-01' and '2000-12-25'
    -- 156 days
  and d_date between '1999-01-01' and '1999-03-31'
  and ss_sold_date_sk between 2451180 and 2451269  -- partition key filter
  group by
    ss_ticket_number,
    ss_customer_sk,
    ss_addr_sk,
    s_city
  ) ms
  join customer on (ms.ss_customer_sk = customer.c_customer_sk)
order by
  c_last_name,
  c_first_name,
  -- substr(s_city, 1, 30),
  city,
  profit
limit 100;
-- end query 1 in stream 0 using template query79.tpl
exit;
