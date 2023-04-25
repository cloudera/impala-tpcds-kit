-- start query 48 in stream 0 using template query48.tpl using seed 1637730879
select sum (ss_quantity)
 from store_sales, store, customer_demographics, customer_address, date_dim
 where s_store_sk = ss_store_sk
 and  ss_sold_date_sk = d_date_sk and d_year = 1999
 and  
 (
  (
   cd_demo_sk = ss_cdemo_sk
   and 
   cd_marital_status = 'D'
   and 
   cd_education_status = 'College'
   and 
   ss_sales_price between 100.00 and 150.00  
   )
 or
  (
  cd_demo_sk = ss_cdemo_sk
   and 
   cd_marital_status = 'W'
   and 
   cd_education_status = 'Secondary'
   and 
   ss_sales_price between 50.00 and 100.00   
  )
 or 
 (
  cd_demo_sk = ss_cdemo_sk
  and 
   cd_marital_status = 'M'
   and 
   cd_education_status = '2 yr Degree'
   and 
   ss_sales_price between 150.00 and 200.00  
 )
 )
 and
 (
  (
  ss_addr_sk = ca_address_sk
  and
  ca_country = 'United States'
  and
  ca_state in ('NE', 'IA', 'NY')
  and ss_net_profit between 0 and 2000  
  )
 or
  (ss_addr_sk = ca_address_sk
  and
  ca_country = 'United States'
  and
  ca_state in ('IN', 'TN', 'OH')
  and ss_net_profit between 150 and 3000 
  )
 or
  (ss_addr_sk = ca_address_sk
  and
  ca_country = 'United States'
  and
  ca_state in ('KS', 'CA', 'CO')
  and ss_net_profit between 50 and 25000 
  )
 )
;

-- end query 48 in stream 0 using template query48.tpl
