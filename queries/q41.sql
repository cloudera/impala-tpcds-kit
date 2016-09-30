
select  distinct(i_product_name)
 from item i1
JOIN 
-- where i_manufact_id between 835 and 835+40
  -- and 
(select i_manufact, count(*) as item_cnt
        from item
        where  (
-- (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and
        (i_color = 'navajo' or i_color = 'bisque') and
        (i_units = 'Box' or i_units = 'Lb') and
        (i_size = 'medium' or i_size = 'small')
        ) or
        (i_category = 'Women' and
        (i_color = 'spring' or i_color = 'dim') and
        (i_units = 'Dram' or i_units = 'Gross') and
        (i_size = 'N/A' or i_size = 'petite')
        ) or
        (i_category = 'Men' and
        (i_color = 'burlywood' or i_color = 'brown') and
        (i_units = 'Tsp' or i_units = 'Gram') and
        (i_size = 'extra large' or i_size = 'economy')
        ) or
        (i_category = 'Men' and
        (i_color = 'forest' or i_color = 'lime') and
        (i_units = 'Ounce' or i_units = 'Case') and
        (i_size = 'medium' or i_size = 'small')
        ))) or
       (
        ((i_category = 'Women' and
        (i_color = 'honeydew' or i_color = 'cornflower') and
        (i_units = 'Pallet' or i_units = 'Bunch') and
        (i_size = 'medium' or i_size = 'small')
        ) or
        (i_category = 'Women' and
        (i_color = 'deep' or i_color = 'peru') and
        (i_units = 'Tbl' or i_units = 'Ton') and
        (i_size = 'N/A' or i_size = 'petite')
        ) or
        (i_category = 'Men' and
        (i_color = 'floral' or i_color = 'violet') and
        (i_units = 'Cup' or i_units = 'Unknown') and
        (i_size = 'extra large' or i_size = 'economy')
        ) or
        (i_category = 'Men' and
        (i_color = 'plum' or i_color = 'green') and
        (i_units = 'Each' or i_units = 'Carton') and
        (i_size = 'medium' or i_size = 'small')
        ))) 
 group by i_manufact) i2
ON i1.i_manufact = i2.i_manufact
where i1.i_manufact_id between  835 and 835+40
and i2.item_cnt > 0
 order by i_product_name
 limit 100;

