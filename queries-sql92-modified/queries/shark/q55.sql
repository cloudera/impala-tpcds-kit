-- start query 1 in stream 0 using template query55.tpl
select
  /*+ MAPJOIN(item, date_dim) */
  i_brand_id,
  i_brand,
  sum(ss_ext_sales_price) ext_price
from
  store_sales
  join item on (store_sales.ss_item_sk = item.i_item_sk)
  join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
where
  i_manager_id = 36
  and d_moy = 12
  and d_year = 2001
  -- and ss_date between '2001-12-01' and '2001-12-31'
  and ss_sold_date_sk between 2452245 and 2452275 -- partition key filter
group by
  i_brand,
  i_brand_id
order by
  ext_price desc,
  i_brand_id
limit 100;
-- end query 1 in stream 0 using template query55.tpl
exit;
