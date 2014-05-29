-- start query 1 in stream 0 using template query89.tpl
select
  *
from
  (select
    /*+ MAPJOIN(item, store, date_dim) */
    i_category,
    i_class,
    i_brand,
    s_store_name,
    s_company_name,
    d_moy,
    sum(ss_sales_price) sum_sales
    -- avg(sum(ss_sales_price)) over (partition by i_category, i_brand, s_store_name, s_company_name) avg_monthly_sales
  from
    store_sales
    join item on (store_sales.ss_item_sk = item.i_item_sk)
    join store on (store_sales.ss_store_sk = store.s_store_sk)
    join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
  where
    -- ss_date between '2000-01-01' and '2000-12-31'
    ss_sold_date_sk between 2451545 and 2451910  -- partition key filter
    and d_year in (2000)
    and ((i_category in('Home', 'Books', 'Electronics')
          and i_class in('wallpaper', 'parenting', 'musical'))
        or (i_category in('Shoes', 'Jewelry', 'Men')
            and i_class in('womens', 'birdal', 'pants'))
        )
  group by
    i_category,
    i_class,
    i_brand,
    s_store_name,
    s_company_name,
    d_moy
  ) tmp1
-- where
--   case when(avg_monthly_sales <> 0) then(abs(sum_sales - avg_monthly_sales) / avg_monthly_sales) else null end > 0.1
order by
  -- sum_sales - avg_monthly_sales,
  sum_sales,
  s_store_name
limit 100;
-- end query 1 in stream 0 using template query89.tpl
exit;
