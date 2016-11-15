
select  i_item_desc 
       ,i_category 
       ,i_class 
       ,i_current_price
       ,sum(cs_ext_sales_price) as itemrevenue 
       ,sum(cs_ext_sales_price)*100/sum(sum(cs_ext_sales_price)) over
           (partition by i_class) as revenueratio
 from catalog_sales
     ,item 
     ,date_dim
 where cs_item_sk = i_item_sk 
   and i_category in ('Sports', 'Women', 'Men')
   and cs_sold_date_sk = d_date_sk
   and cs_sold_date_sk between 2451628 and 2451658
   and d_date between cast('2000-03-24' as timestamp) 
 				and (cast('2000-03-24' as timestamp) + interval 30 days)
 group by i_item_id
         ,i_item_desc 
         ,i_category
         ,i_class
         ,i_current_price
 order by i_category
         ,i_class
         ,i_item_id
         ,i_item_desc
         ,revenueratio
limit 100;

