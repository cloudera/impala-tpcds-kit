-- start query 1 in stream 0 using template query27.tpl
select
  /*+ MAPJOIN(store, customer_demographics, item, date_dim) */
  i_item_id,
  s_state,
  -- grouping(s_state) g_state,
  avg(ss_quantity) agg1,
  avg(ss_list_price) agg2,
  avg(ss_coupon_amt) agg3,
  avg(ss_sales_price) agg4
from
  store_sales
  join store on (store_sales.ss_store_sk = store.s_store_sk)
  join customer_demographics on (store_sales.ss_cdemo_sk = customer_demographics.cd_demo_sk)
  join item on (store_sales.ss_item_sk = item.i_item_sk)
  join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
where
  -- ss_date between '1998-01-01' and '1998-12-31'
  ss_sold_date_sk between 2450815 and 2451179  -- partition key filter
  and d_year = 1998
  and s_state in ('WI', 'CA', 'TX', 'FL', 'WA', 'TN')
  and cd_gender = 'F'
  and cd_marital_status = 'W'
  and cd_education_status = 'Primary'
group by
  -- rollup(i_item_id, s_state)
  i_item_id,
  s_state
order by
  i_item_id,
  s_state 
limit 100;
