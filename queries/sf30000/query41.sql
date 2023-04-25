-- start query 41 in stream 0 using template query41.tpl using seed 117881082
select  distinct(i_product_name)
 from item i1
 where i_manufact_id between 917 and 917+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'antique' or i_color = 'pale') and 
        (i_units = 'Tbl' or i_units = 'Case') and
        (i_size = 'small' or i_size = 'extra large')
        ) or
        (i_category = 'Women' and
        (i_color = 'snow' or i_color = 'lemon') and
        (i_units = 'Box' or i_units = 'Ounce') and
        (i_size = 'economy' or i_size = 'N/A')
        ) or
        (i_category = 'Men' and
        (i_color = 'green' or i_color = 'blue') and
        (i_units = 'Gross' or i_units = 'Ton') and
        (i_size = 'large' or i_size = 'petite')
        ) or
        (i_category = 'Men' and
        (i_color = 'cream' or i_color = 'frosted') and
        (i_units = 'Bundle' or i_units = 'Gram') and
        (i_size = 'small' or i_size = 'extra large')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'orange' or i_color = 'spring') and 
        (i_units = 'Lb' or i_units = 'Carton') and
        (i_size = 'small' or i_size = 'extra large')
        ) or
        (i_category = 'Women' and
        (i_color = 'lawn' or i_color = 'violet') and
        (i_units = 'Oz' or i_units = 'Cup') and
        (i_size = 'economy' or i_size = 'N/A')
        ) or
        (i_category = 'Men' and
        (i_color = 'navy' or i_color = 'linen') and
        (i_units = 'Pound' or i_units = 'Unknown') and
        (i_size = 'large' or i_size = 'petite')
        ) or
        (i_category = 'Men' and
        (i_color = 'almond' or i_color = 'olive') and
        (i_units = 'Pallet' or i_units = 'Bunch') and
        (i_size = 'small' or i_size = 'extra large')
        )))) > 0
 order by i_product_name
 limit 100;

-- end query 41 in stream 0 using template query41.tpl
