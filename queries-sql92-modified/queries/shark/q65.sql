--q65
-- start query 1 in stream 0 using template query65.tpl
select
  s_store_name,
  i_item_desc,
  sc.revenue,
  i_current_price,
  i_wholesale_cost,
  i_brand
from
  (select
    /*+ MAPJOIN(date_dim) */
    ss_store_sk,
    ss_item_sk,
    sum(ss_sales_price) as revenue
  from
    store_sales
    join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
  where
    -- ss_date between '2001-01-01' and '2001-12-31'
    ss_sold_date_sk between 2451911 and 2452275  -- partition key filter
    and d_month_seq between 1212 and 1212 + 11
  group by
    ss_store_sk,
    ss_item_sk
  ) sc
  join item on (sc.ss_item_sk = item.i_item_sk)
  join store on (sc.ss_store_sk = store.s_store_sk)
  join 
  (select
    ss_store_sk,
    avg(revenue) as ave
  from
    (select
      /*+ MAPJOIN(date_dim) */
      ss_store_sk,
      ss_item_sk,
      sum(ss_sales_price) as revenue
    from
      store_sales
      join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
    where
      -- ss_date between '2001-01-01' and '2001-12-31'
      ss_sold_date_sk between 2451911 and 2452275  -- partition key filter
      and d_month_seq between 1212 and 1212 + 11
    group by
      ss_store_sk,
      ss_item_sk
    ) sa
  group by
    ss_store_sk
  ) sb on (sc.ss_store_sk = sb.ss_store_sk) -- 676 rows
where
  sc.revenue <= 0.1 * sb.ave
order by
  s_store_name,
  i_item_desc 
limit 100;
-- end query 1 in stream 0 using template query65.tpl
exit;
