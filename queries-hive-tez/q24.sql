with ssales as
(select c_last_name
      ,c_first_name
      ,s_store_name
      ,ca_state
      ,s_state
      ,i_color
      ,i_current_price
      ,i_manager_id
      ,i_units
      ,i_size
      ,sum(ss_ext_sales_price) netpaid
from (select * from store_sales, store, item, customer, customer_address where 
     ss_item_sk = i_item_sk and
	ss_store_sk = s_store_sk and
	ss_customer_sk = c_customer_sk and c_birth_country = upper(ca_country) and
	s_zip = ca_zip and s_market_id=5 
	) ss1
    join store_returns on 
  ss_ticket_number = sr_ticket_number
  and ss_item_sk = sr_item_sk
group by c_last_name
        ,c_first_name
        ,s_store_name
        ,ca_state
        ,s_state
        ,i_color
        ,i_current_price
        ,i_manager_id
        ,i_units
        ,i_size)
select c_last_name, c_first_name, s_store_name, paid
from (
 select c_last_name
       ,c_first_name
       ,s_store_name
       ,sum(netpaid) paid
 from ssales
 where i_color = 'azure'
  group by c_last_name
         ,c_first_name
         ,s_store_name
 ) a1
 cross join
 (select 0.05*avg(netpaid) paid2
 from ssales
 ) a2
 where a1.paid > a2.paid2
limit 100
;



