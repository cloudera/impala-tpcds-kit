-- start query 23 in stream 0 using template query23.tpl
with frequent_ss_items as
 (select substr(i_item_desc,1,30) itemdesc,i_item_sk item_sk,d_date solddate,count(*) cnt
  from store_sales
      ,date_dim
      ,item
  where ss_sold_date_sk = d_date_sk
    and ss_sold_date_sk between 2451180 and 2452640
    and ss_item_sk = i_item_sk
    and d_year in (1999,1999 + 1,1999 + 2,1999 + 3)
  group by substr(i_item_desc,1,30),i_item_sk,d_date
  having cnt >4),
 max_store_sales as
 (select max(csales) tpcds_cmax
  from (select c_customer_sk,sum(ss_quantity*ss_sales_price) csales
        from store_sales
            ,customer
            ,date_dim 
        where ss_customer_sk = c_customer_sk
         and ss_sold_date_sk = d_date_sk
	 and ss_sold_date_sk between 2451180 and 2452640
         and d_year in (1999,1999+1,1999+2,1999+3)
        group by c_customer_sk) x),
 best_ss_customer as
 (select t1.* from (
select c_customer_sk,sum(ss_quantity*ss_sales_price) ssales
from store_sales ,customer
where ss_customer_sk = c_customer_sk
group by c_customer_sk) t1,
(select tpcds_cmax * 95/100 as c1 from max_store_sales) t2
where t1.ssales > t2.c1)
 select  c_last_name,c_first_name,sales
 from (select c_last_name,c_first_name,sum(cs_quantity*cs_list_price) sales
        from catalog_sales cs
            ,customer
            ,date_dim 
            ,frequent_ss_items
            ,best_ss_customer
        where d_year = 1999 
         and d_moy = 3 
         and cs_sold_date_sk = d_date_sk 
         and cs_sold_date_sk between 2451239 and 2451269
         and cs_item_sk = frequent_ss_items.item_sk
         and cs_bill_customer_sk =  best_ss_customer.c_customer_sk
         and cs_bill_customer_sk = customer.c_customer_sk 
       group by c_last_name,c_first_name
      union all
      select c_last_name,c_first_name,sum(ws_quantity*ws_list_price) sales
       from web_sales
           ,customer
           ,date_dim 
           ,frequent_ss_items
           ,best_ss_customer
       where d_year = 1999 
         and d_moy = 3 
         and ws_sold_date_sk = d_date_sk 
         and ws_sold_date_sk between 2451239 and 2451269
         and ws_item_sk =frequent_ss_items.item_sk
         and ws_bill_customer_sk =best_ss_customer.c_customer_sk
         and ws_bill_customer_sk = customer.c_customer_sk
       group by c_last_name,c_first_name) y
     order by c_last_name,c_first_name,sales
  limit 100;

-- end query 23 in stream 0 using template query23.tpl
