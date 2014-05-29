-- start query 1 in stream 0 using template query7.tpl
select
  /*+ MAPJOIN(customer_demographics, item, promotion, date_dim) */
  i_item_id,
  avg(ss_quantity) agg1,
  avg(ss_list_price) agg2,
  avg(ss_coupon_amt) agg3,
  avg(ss_sales_price) agg4
from
  store_sales
  join customer_demographics on (store_sales.ss_cdemo_sk = customer_demographics.cd_demo_sk)
  join item on (store_sales.ss_item_sk = item.i_item_sk)
  join promotion on (store_sales.ss_promo_sk = promotion.p_promo_sk)
  join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
where
  cd_gender = 'F'
  and cd_marital_status = 'W'
  and cd_education_status = 'Primary'
  and (p_channel_email = 'N'
    or p_channel_event = 'N')
  and d_year = 1998
  -- and ss_date between '1998-01-01' and '1998-12-31'
  and ss_sold_date_sk between 2450815 and 2451179 -- partition key filter
group by
  i_item_id
order by
  i_item_id 
limit 100;
