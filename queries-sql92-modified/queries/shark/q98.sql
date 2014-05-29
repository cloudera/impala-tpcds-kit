select 
  /*+ MAPJOIN(item, date_dim) */
  i_item_desc,
  i_category,
  i_class,
  i_current_price,
  i_item_id,
  sum(ss_ext_sales_price) as itemrevenue
  -- sum(ss_ext_sales_price) * 100 / sum(sum(ss_ext_sales_price)) over (partition by i_class) as revenueratio
from
  store_sales
  join item on (store_sales.ss_item_sk = item.i_item_sk)
  join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
where
  i_category in('Jewelry', 'Sports', 'Books')
  -- and d_date between cast('2001-01-12' as date) and (cast('2001-01-12' as date) + 30)
  -- and d_date between '2001-01-12' and '2001-02-11'
  -- and ss_date between '2001-01-12' and '2001-02-11'
  -- and ss_sold_date_sk between 2451922 and 2451952  -- partition key filter
  and ss_sold_date_sk between 2451911 and 2451941  -- partition key filter (1 calendar month)
  and d_date between '2001-01-01' and '2001-01-31'
group by
  i_item_id,
  i_item_desc,
  i_category,
  i_class,
  i_current_price
order by
  i_category,
  i_class,
  i_item_id,
  i_item_desc
  -- revenueratio
limit 1000;

