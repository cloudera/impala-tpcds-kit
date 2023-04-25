-- start query 41 in stream 0 using template query41.tpl using seed 1004413187
select  distinct(i_product_name)
 from item i1
 where i_manufact_id between 707 and 707+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'light' or i_color = 'lemon') and 
        (i_units = 'Each' or i_units = 'Bunch') and
        (i_size = 'medium' or i_size = 'extra large')
        ) or
        (i_category = 'Women' and
        (i_color = 'lawn' or i_color = 'slate') and
        (i_units = 'Ton' or i_units = 'Gross') and
        (i_size = 'large' or i_size = 'N/A')
        ) or
        (i_category = 'Men' and
        (i_color = 'peach' or i_color = 'ivory') and
        (i_units = 'Carton' or i_units = 'Ounce') and
        (i_size = 'petite' or i_size = 'economy')
        ) or
        (i_category = 'Men' and
        (i_color = 'cornflower' or i_color = 'chiffon') and
        (i_units = 'Case' or i_units = 'Oz') and
        (i_size = 'medium' or i_size = 'extra large')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'forest' or i_color = 'azure') and 
        (i_units = 'Pallet' or i_units = 'Tsp') and
        (i_size = 'medium' or i_size = 'extra large')
        ) or
        (i_category = 'Women' and
        (i_color = 'navy' or i_color = 'papaya') and
        (i_units = 'Box' or i_units = 'N/A') and
        (i_size = 'large' or i_size = 'N/A')
        ) or
        (i_category = 'Men' and
        (i_color = 'turquoise' or i_color = 'ghost') and
        (i_units = 'Dozen' or i_units = 'Dram') and
        (i_size = 'petite' or i_size = 'economy')
        ) or
        (i_category = 'Men' and
        (i_color = 'coral' or i_color = 'red') and
        (i_units = 'Cup' or i_units = 'Tbl') and
        (i_size = 'medium' or i_size = 'extra large')
        )))) > 0
 order by i_product_name
 limit 100;

-- end query 41 in stream 0 using template query41.tpl
