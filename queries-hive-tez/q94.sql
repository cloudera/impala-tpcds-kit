select
   count(distinct ws_order_number) as order_count
  ,sum(ws_ext_ship_cost) as total_shipping_cost
  ,sum(ws_net_profit) as total_net_profit
from 
(select ws1.ws_order_number,ws1.ws_ext_ship_cost,ws1.ws_net_profit
from
   web_sales ws1
  ,date_dim
  ,customer_address
  ,web_site
,web_sales ws2
where
    d_date between '2001-05-01' and '2001-07-01'
and ws1.ws_ship_date_sk = d_date_sk
and ws1.ws_ship_addr_sk = ca_address_sk
and ca_state = 'KY'
and ws1.ws_web_site_sk = web_site_sk
and web_company_name = 'pri'
and ws1.ws_order_number = ws2.ws_order_number
and ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk) bla
left outer join web_returns wr1
on ws_order_number = wr1.wr_order_number
where wr1.wr_order_number is null
order by order_count
limit 100;
