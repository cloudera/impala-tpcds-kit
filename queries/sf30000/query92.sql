-- start query 92 in stream 0 using template query92.tpl using seed 2063897006
select  
   sum(ws_ext_discount_amt)  as "Excess Discount Amount" 
from 
    web_sales 
   ,item 
   ,date_dim
where
i_manufact_id = 7
and i_item_sk = ws_item_sk 
and d_date between cast('2000-01-16' as date) and
        (cast('2000-01-16' as date) + interval 90 days)
and d_date_sk = ws_sold_date_sk 
and ws_ext_discount_amt  
     > ( 
         SELECT 
            1.3 * avg(ws_ext_discount_amt) 
         FROM 
            web_sales 
           ,date_dim
         WHERE 
              ws_item_sk = i_item_sk 
          and d_date between cast('2000-01-16' as date) and
                             (cast('2000-01-16' as date) + interval 90 days)
          and d_date_sk = ws_sold_date_sk 
      ) 
order by sum(ws_ext_discount_amt)
limit 100;

-- end query 92 in stream 0 using template query92.tpl
