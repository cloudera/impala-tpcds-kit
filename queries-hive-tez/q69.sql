with ss as (select 
            ss_customer_sk
        from
            store_sales,
            date_dim
        where ss_sold_date_sk = d_date_sk
                and ss_sold_date_sk between 2452700 and 2452791
                and d_year = 2003
                and d_moy between 3 and 3 + 2)
select 
    cd_gender,
    cd_marital_status,
    cd_education_status,
    count(*) cnt1,
    cd_purchase_estimate,
    count(*) cnt2,
    cd_credit_rating,
    count(*) cnt3
from
    customer c,
    customer_address ca,
    customer_demographics,ss
where c.c_current_addr_sk = ca.ca_address_sk
        and ca_state in ('WY' , 'UT', 'MI')
        and cd_demo_sk = c.c_current_cdemo_sk
        and ss.ss_customer_sk = c.c_customer_sk and c.c_customer_sk not in ( select 
            ws_bill_customer_sk customer_sk
        from
            web_sales,
            date_dim
        where
    ws_sold_date_sk = d_date_sk
                and ws_sold_date_sk between 2452700 and 2452791 and d_year = 2003
                and d_moy between 3 and 5
        union select cs_ship_customer_sk customer_sk
            from catalog_sales,
            date_dim where
    cs_sold_date_sk = d_date_sk
                and cs_sold_date_sk between 2452700 and 2452791
                and d_year = 2003
                and d_moy between 3 and 5)
group by cd_gender , cd_marital_status , cd_education_status , cd_purchase_estimate , cd_credit_rating
order by cd_gender , cd_marital_status , cd_education_status , cd_purchase_estimate , cd_credit_rating
limit 100;
