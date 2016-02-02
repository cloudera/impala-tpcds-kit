with item_avg_price as (select 
    avg(i_current_price) avg_current_price, i_category
from
    item
group by i_category) 


select * from (select 
    a.ca_state state, count(*) cnt
from
    customer_address a,
    customer c,
    store_sales s,
    date_dim d,
    item i,
    item_avg_price
where
    a.ca_address_sk = c.c_current_addr_sk
        and c.c_customer_sk = s.ss_customer_sk
        and s.ss_sold_date_sk = d.d_date_sk
	and s.ss_sold_date_sk between 2451180 and 2451210
        and s.ss_item_sk = i.i_item_sk
        and item_avg_price.i_category = i.i_category
        and d.d_month_seq in (select distinct
            (d_month_seq)
        from
            date_dim
        where
            d_year = 1999 and d_moy = 1)
        and i.i_current_price > 1.2 * item_avg_price.avg_current_price
group by a.ca_state) bla
where cnt >= 10
order by cnt
limit 100;
