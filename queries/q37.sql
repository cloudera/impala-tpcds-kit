
select  i_item_id
       ,i_item_desc
       ,i_current_price
 from 
 	item, inventory, date_dim, catalog_sales
 where 
	 i_current_price between 19 and 19 + 30
	 and inv_item_sk = i_item_sk
	 and d_date_sk=inv_date_sk
	 and d_date between cast('2000-03-27' as timestamp) and (cast('2000-03-27' as timestamp) +  interval 60 days)
	 and i_manufact_id in (874,844,819,868)
	 and inv_quantity_on_hand between 100 and 500
	 and cs_item_sk = i_item_sk
 group by i_item_id,i_item_desc,i_current_price
 order by i_item_id
 limit 100;
