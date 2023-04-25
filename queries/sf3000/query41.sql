-- start query 41 in stream 0 using template query41.tpl using seed 1813495740
select  distinct(i_product_name)
 from item i1
 where i_manufact_id between 732 and 732+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'beige' or i_color = 'spring') and 
        (i_units = 'Tsp' or i_units = 'Ton') and
        (i_size = 'petite' or i_size = 'extra large')
        ) or
        (i_category = 'Women' and
        (i_color = 'white' or i_color = 'pale') and
        (i_units = 'Box' or i_units = 'Dram') and
        (i_size = 'large' or i_size = 'economy')
        ) or
        (i_category = 'Men' and
        (i_color = 'midnight' or i_color = 'frosted') and
        (i_units = 'Bunch' or i_units = 'Carton') and
        (i_size = 'small' or i_size = 'N/A')
        ) or
        (i_category = 'Men' and
        (i_color = 'azure' or i_color = 'goldenrod') and
        (i_units = 'Pallet' or i_units = 'Gross') and
        (i_size = 'petite' or i_size = 'extra large')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'brown' or i_color = 'hot') and 
        (i_units = 'Tbl' or i_units = 'Cup') and
        (i_size = 'petite' or i_size = 'extra large')
        ) or
        (i_category = 'Women' and
        (i_color = 'powder' or i_color = 'honeydew') and
        (i_units = 'Bundle' or i_units = 'Unknown') and
        (i_size = 'large' or i_size = 'economy')
        ) or
        (i_category = 'Men' and
        (i_color = 'antique' or i_color = 'purple') and
        (i_units = 'N/A' or i_units = 'Dozen') and
        (i_size = 'small' or i_size = 'N/A')
        ) or
        (i_category = 'Men' and
        (i_color = 'lavender' or i_color = 'tomato') and
        (i_units = 'Lb' or i_units = 'Oz') and
        (i_size = 'petite' or i_size = 'extra large')
        )))) > 0
 order by i_product_name
 limit 100;

-- end query 41 in stream 0 using template query41.tpl
