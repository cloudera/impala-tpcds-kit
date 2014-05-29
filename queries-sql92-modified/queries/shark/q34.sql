-- start query 1 in stream 0 using template query34.tpl
select
  c_last_name,
  c_first_name,
  c_salutation,
  c_preferred_cust_flag,
  ss_ticket_number,
  cnt
from
  (select
    /*+ MAPJOIN(household_demographics, store, date_dim) */
    ss_ticket_number,
    ss_customer_sk,
    count(*) cnt
  from
    store_sales
    join household_demographics on (store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk)
    join store on (store_sales.ss_store_sk = store.s_store_sk)
    join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
  where
    date_dim.d_year in (1998, 1998 + 1, 1998 + 2)
    and (date_dim.d_dom between 1 and 3
      or date_dim.d_dom between 25 and 28)
    and (household_demographics.hd_buy_potential = '>10000'
      or household_demographics.hd_buy_potential = 'unknown')
    and household_demographics.hd_vehicle_count > 0
    and (case when household_demographics.hd_vehicle_count > 0 then household_demographics.hd_dep_count / household_demographics.hd_vehicle_count else null end) > 1.2
    and store.s_county in ('Saginaw County', 'Sumner County', 'Appanoose County', 'Daviess County', 'Fairfield County', 'Raleigh County', 'Ziebach County', 'Williamson County') 
    and ss_sold_date_sk between 2450816 and 2451910 -- partition key filter
  group by
    ss_ticket_number,
    ss_customer_sk
  ) dn
join customer on (dn.ss_customer_sk = customer.c_customer_sk)
where
  cnt between 15 and 20
order by
  c_last_name,
  c_first_name,
  c_salutation,
  c_preferred_cust_flag desc
limit 1000;
-- end query 1 in stream 0 using template query34.tpl
exit;
