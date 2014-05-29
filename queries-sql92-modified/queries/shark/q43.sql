-- start query 1 in stream 0 using template query43.tpl
select
  /*+ MAPJOIN(store, date_dim) */
  s_store_name,
  s_store_id,
  sum(case when (d_day_name = 'Sunday') then ss_sales_price else null end) sun_sales,
  sum(case when (d_day_name = 'Monday') then ss_sales_price else null end) mon_sales,
  sum(case when (d_day_name = 'Tuesday') then ss_sales_price else null end) tue_sales,
  sum(case when (d_day_name = 'Wednesday') then ss_sales_price else null end) wed_sales,
  sum(case when (d_day_name = 'Thursday') then ss_sales_price else null end) thu_sales,
  sum(case when (d_day_name = 'Friday') then ss_sales_price else null end) fri_sales,
  sum(case when (d_day_name = 'Saturday') then ss_sales_price else null end) sat_sales
from
  store_sales
  join store on (store_sales.ss_store_sk = store.s_store_sk)
  join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
where
  s_gmt_offset = -5
  and d_year = 1998
  -- and ss_date between '1998-01-01' and '1998-12-31'
  and ss_sold_date_sk between 2450816 and 2451179  -- partition key filter
group by
  s_store_name,
  s_store_id
order by
  s_store_name,
  s_store_id,
  sun_sales,
  mon_sales,
  tue_sales,
  wed_sales,
  thu_sales,
  fri_sales,
  sat_sales 
limit 100;
-- end query 1 in stream 0 using template query43.tpl
exit;
