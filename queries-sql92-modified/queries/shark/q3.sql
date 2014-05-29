-- start query 1 in stream 0 using template query3.tpl
select
  /*+ MAPJOIN(item, date_dim) */
  d_year,
  -- year(ss_date) as d_year,
  -- case 
  --   when ss_sold_date_sk between 2451149 and 2451179 then 1998
  --   when ss_sold_date_sk between 2451514 and 2451544 then 1999
  --   when ss_sold_date_sk between 2451880 and 2451910 then 2000
  --   when ss_sold_date_sk between 2452245 and 2452275 then 2001
  --   when ss_sold_date_sk between 2452610 and 2452640 then 2002
  -- end as d_year,
  item.i_brand_id brand_id,
  item.i_brand brand,
  sum(ss_ext_sales_price) sum_agg
from
  store_sales
  join item on (store_sales.ss_item_sk = item.i_item_sk)
  join date_dim dt on (dt.d_date_sk = store_sales.ss_sold_date_sk)
where
  item.i_manufact_id = 436
  and dt.d_moy = 12
  -- and (ss_date between '1998-12-01' and '1998-12-31'
  --   or ss_date between '1999-12-01' and '1999-12-31'
  --   or ss_date between '2000-12-01' and '2000-12-31'
  --   or ss_date between '2001-12-01' and '2001-12-31'
  --   or ss_date between '2002-12-01' and '2002-12-31')
  and (ss_sold_date_sk between 2451149 and 2451179
    or ss_sold_date_sk between 2451514 and 2451544
    or ss_sold_date_sk between 2451880 and 2451910
    or ss_sold_date_sk between 2452245 and 2452275
    or ss_sold_date_sk between 2452610 and 2452640)
group by
  d_year,
  item.i_brand,
  item.i_brand_id
order by
  d_year,
  sum_agg desc,
  brand_id
limit 100;
