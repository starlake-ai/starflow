
select customer_id, sum(amount) as total_amount, count(*) as order_count
from "shop"."staging"."stg_orders"
where amount >= 0
group by customer_id