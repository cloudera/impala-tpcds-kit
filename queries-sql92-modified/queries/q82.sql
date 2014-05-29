-- start query 1 in stream 0 using template query82.tpl
select
  i_item_id,
  i_item_desc,
  i_current_price
from
  store_sales
  join item on (store_sales.ss_item_sk = item.i_item_sk)
  join inventory on (item.i_item_sk = inventory.inv_item_sk)
  -- join date_dim on (inventory.inv_date_sk = date_dim.d_date_sk)
where
  i_current_price between 30 and 30 + 30
  and i_manufact_id in (437, 129, 727, 663)
  and inv_quantity_on_hand between 100 and 500
  and inv_date between '2002-05-30' and '2002-07-29'
  -- and d_date between cast('2002-05-30' as date) and (cast('2002-05-30' as date) + 60)
group by
  i_item_id,
  i_item_desc,
  i_current_price
order by
  i_item_id 
limit 100;
-- end query 1 in stream 0 using template query82.tpl
