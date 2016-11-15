
select t1.ca_zip, t1.ca_city, sum(t1.ws_sales_price) from (
(
select  ca_zip, ca_city, ws_sales_price
 from web_sales, customer ,  customer_address, date_dim
 where (ws_bill_customer_sk = c_customer_sk
 	and c_current_addr_sk = ca_address_sk 
	and substr(ca_zip,1,5) in ('85669', '86197','88274','83405','86475', '85392', '85460', '80348', '81792')
 	and ws_sold_date_sk = d_date_sk
	and ws_sold_date_sk between 2452367 and 2452457
 	and d_qoy = 2 and d_year = 2002)
 group by ca_zip, ca_city, ws_sales_price
 order by ca_zip, ca_city, ws_sales_price
)
union
(
select  ca_zip, ca_city, ws_sales_price
from web_sales, customer , customer_address, item, date_dim
where (ws_bill_customer_sk = c_customer_sk
        and c_current_addr_sk = ca_address_sk
 	and ws_item_sk = i_item_sk 
        and  i_item_id in (select i_item_id
            from item
            where i_item_sk in (2, 3, 5, 7, 11, 13, 17, 19, 23, 29)
            )
        and ws_sold_date_sk = d_date_sk
        and ws_sold_date_sk between 2452367 and 2452457
        and d_qoy = 2 and d_year = 2002)
 group by ca_zip, ca_city, ws_sales_price
 order by ca_zip, ca_city, ws_sales_price
)
) t1
 group by ca_zip, ca_city 
 order by ca_zip, ca_city 
;

