-- start query 41 in stream 0 using template query41.tpl using seed 103957553
select  distinct(i_product_name)
 from item i1
 where i_manufact_id between 697 and 697+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'bisque' or i_color = 'peach') and 
        (i_units = 'Pound' or i_units = 'Each') and
        (i_size = 'medium' or i_size = 'small')
        ) or
        (i_category = 'Women' and
        (i_color = 'green' or i_color = 'white') and
        (i_units = 'Carton' or i_units = 'Oz') and
        (i_size = 'petite' or i_size = 'N/A')
        ) or
        (i_category = 'Men' and
        (i_color = 'light' or i_color = 'tomato') and
        (i_units = 'Case' or i_units = 'Tsp') and
        (i_size = 'economy' or i_size = 'large')
        ) or
        (i_category = 'Men' and
        (i_color = 'floral' or i_color = 'black') and
        (i_units = 'Dram' or i_units = 'Dozen') and
        (i_size = 'medium' or i_size = 'small')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'khaki' or i_color = 'papaya') and 
        (i_units = 'Cup' or i_units = 'Ounce') and
        (i_size = 'medium' or i_size = 'small')
        ) or
        (i_category = 'Women' and
        (i_color = 'goldenrod' or i_color = 'firebrick') and
        (i_units = 'Ton' or i_units = 'Gross') and
        (i_size = 'petite' or i_size = 'N/A')
        ) or
        (i_category = 'Men' and
        (i_color = 'peru' or i_color = 'almond') and
        (i_units = 'Gram' or i_units = 'N/A') and
        (i_size = 'economy' or i_size = 'large')
        ) or
        (i_category = 'Men' and
        (i_color = 'thistle' or i_color = 'mint') and
        (i_units = 'Bundle' or i_units = 'Bunch') and
        (i_size = 'medium' or i_size = 'small')
        )))) > 0
 order by i_product_name
 limit 100;

-- end query 41 in stream 0 using template query41.tpl
