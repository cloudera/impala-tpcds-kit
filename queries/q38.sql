
select
count(*)
            from
              (select c_last_name,c_first_name,d_date, sum(q18.c3) c3,count(*) c4
               from
                 (
                  select
                    c_last_name,c_first_name,d_date,1 as c3
                  from
                    (select c_last_name,c_first_name,d_date	
                     from
                       (select c_last_name, c_first_name, d_date, q14.c3 c3, q14.c4 c4
                        from
                          (select
                             c_last_name,c_first_name,d_date,sum(q13.c3) c3,count(*) c4
                           from
                             (
                              select c_last_name,c_first_name,d_date,1 as c3
                              from
                                customer, date_dim, store_sales
                              where
                                (d_month_seq between 1215 and 1226) and
                                (ss_customer_sk = c_customer_sk) and
                                (ss_sold_date_sk = d_date_sk) and
                    			(ss_sold_date_sk between 2452001 and 2452365)
                              union all
                              select c_last_name,c_first_name,d_date,-1 as c3
                              from
                                customer, date_dim, catalog_sales 
                              where
                                (d_month_seq between 1215 and 1226) and
                                (cs_bill_customer_sk = c_customer_sk) and
                                (cs_sold_date_sk = d_date_sk) and
                    			(cs_sold_date_sk between 2452001 and 2452365)
                             ) as q13
                           group by
                             c_last_name,
                             c_first_name,
                             d_date
                          ) as q14
                        where
                          ((q14.c4 - case when (q14.c3 >= 0) then q14.c3 else -(q14.c3) end) >= 2)
                       ) as q15
                    ) as q16
                    union all
					select c_last_name,c_first_name,d_date,-1 as c3
                  	from
                    	customer,date_dim,web_sales 
                  	where
	                    (d_month_seq between 1215 and 1226) and
	                    (ws_bill_customer_sk = c_customer_sk) and
	                    (ws_sold_date_sk = d_date_sk) and
	                    (ws_sold_date_sk between 2452001 and 2452365)
                    ) as q18
               group by
                 c_last_name,
                 c_first_name,
                 d_date
              ) as q19
            where
              ((q19.c4 - case when (q19.c3 >= 0) then q19.c3 else -(q19.c3) end) >= 2)
;

