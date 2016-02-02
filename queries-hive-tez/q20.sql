select  i_item_desc 
       ,i_category 
       ,i_class 
       ,i_current_price
       ,i_item_id
       ,sum(cs_ext_sales_price) as itemrevenue 
       ,sum(cs_ext_sales_price)*100/sum(sum(cs_ext_sales_price)) over
           (partition by i_class) as revenueratio
 from	catalog_sales
     ,item 
     ,date_dim
 where catalog_sales.cs_item_sk = item.i_item_sk 
   and i_category in ('Sports', 'Women', 'Men')
   and catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
   and cs_sold_date_sk between 2451628 and 2451658
 and d_date between '2000-03-24' and '2000-04-24'
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
