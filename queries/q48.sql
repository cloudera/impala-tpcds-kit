
select sum (ss_quantity)
from store_sales, store, (select * from customer_demographics where cd_marital_status = 'S' and cd_education_status = '4 yr Degree') as v1, (select * from customer_address where ca_country = 'United States' and ca_state in ('AK', 'IA', 'NE', 'NY', 'VA', 'AR', 'AZ', 'MI', 'NC')) as v2, date_dim
 where s_store_sk = ss_store_sk
 and  ss_sold_date_sk = d_date_sk and d_year = 1998
 and  ss_sold_date_sk between 2450815 and 2451179
 and  (v1.cd_demo_sk = ss_cdemo_sk and
        (
        (ss_sales_price between 100.00 and 150.00) or
        (ss_sales_price between 50.00 and 100.00) or
        (ss_sales_price between 150.00 and 200.00)
        )
      )
 and  (ss_addr_sk = v2.ca_address_sk and
      (
        (ca_state in ('AK', 'IA', 'NE') and ss_net_profit between 0 and 2000)
        or (ca_state in ('NY', 'VA', 'AR') and ss_net_profit between 150 and 3000)
        or (ca_state in ('AZ', 'MI', 'NC') and ss_net_profit between 50 and 25000)
      )
      )
;


