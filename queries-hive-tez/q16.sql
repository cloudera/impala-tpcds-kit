select
   count(distinct bla.cs_order_number) as order_count
  ,sum(bla.cs_ext_ship_cost) as total_shipping_cost
  ,sum(bla.cs_net_profit) as total_net_profit from 
(select cs1.cs_order_number, cs1.cs_ext_ship_cost,cs1.cs_net_profit
from
   catalog_sales cs1
  ,date_dim
  ,customer_address
  ,call_center
  ,catalog_sales cs2
where
    d_date between '2000-02-01'  and
           '2000-02-01'
and cs1.cs_ship_date_sk = d_date_sk
and cs1.cs_ship_addr_sk = ca_address_sk
and ca_state = 'TX'
and cs1.cs_call_center_sk = cc_call_center_sk
and cc_county in ('Williamson County','Walker County','Ziebach County','Ziebach County',
                  'Ziebach County')
and cs1.cs_order_number = cs2.cs_order_number
and cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk) bla
left outer join catalog_returns cr1
on cs_order_number = cr1.cr_order_number where cr1.cr_order_number is null
order by order_count
limit 100;
