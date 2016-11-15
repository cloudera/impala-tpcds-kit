

select  asceding.rnk, i1.i_product_name best_performing, i2.i_product_name worst_performing
from(select *
     from (select item_sk,rank() over (order by rank_col asc) rnk
           from (
		select  item_sk, rank_col from 
			(select ss_item_sk item_sk,avg(ss_net_profit) rank_col
			from store_sales ss1
			where ss_store_sk = 218
			group by ss_item_sk) a1 cross join
			( select avg(ss_net_profit) * 0.9 cmp_col
  			from store_sales
  			where ss_store_sk = 218
  			and ss_hdemo_sk is null
 			group by ss_store_sk limit 1
 			) a2
  			where a1.rank_col > a2.cmp_col
		)
		V1)V11
     where rnk  < 11) asceding,
    (select *
     from (select item_sk,rank() over (order by rank_col desc) rnk
           from 
		(
		select item_sk, rank_col from 
			(select ss_item_sk item_sk,avg(ss_net_profit) rank_col
			from store_sales ss1
			where ss_store_sk = 218
			group by ss_item_sk) a1 
			cross join
			(select avg(ss_net_profit) * 0.9 cmp_col
  			from store_sales
  			where ss_store_sk = 218
  			and ss_hdemo_sk is null
  			group by ss_store_sk limit 1
 			) a2
 			where a1.rank_col > a2.cmp_col
		)
		V1)V11
     where rnk  < 11) descending,
item i1,
item i2
where asceding.rnk = descending.rnk
  and i1.i_item_sk=asceding.item_sk
  and i2.i_item_sk=descending.item_sk
order by asceding.rnk
limit 100;


