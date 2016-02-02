with customer_total_return as
(select sr_customer_sk as ctr_customer_sk
  ,sr_store_sk as ctr_store_sk
  ,sum(SR_RETURN_TAX) as ctr_total_return
from store_returns
,date_dim
where sr_returned_date_sk = d_date_sk
and d_year =2002
and sr_returned_date_sk between 2452276 and 2452640
group by sr_customer_sk
,sr_store_sk),
customer_avg_return as 
(select avg(ctr_total_return)*1.2 ctr_avg_return,ctr_store_sk
from customer_total_return
group by ctr_store_sk)
select  c_customer_id
from customer_total_return ctr1
,store
,customer,
customer_avg_return ctr2
where ctr1.ctr_store_sk = ctr2.ctr_store_sk
and ctr1.ctr_total_return > ctr2.ctr_avg_return
and s_store_sk = ctr1.ctr_store_sk
and s_state = 'LA'
and ctr1.ctr_customer_sk = c_customer_sk
order by c_customer_id
limit 100;

