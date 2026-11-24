with __dbt__cte__int_order_totals as (

select customer_id, sum(amount) as total_amount, count(*) as order_count
from "shop"."staging"."stg_orders"
where amount >= 0
group by customer_id
) select c.customer_id, coalesce(t.total_amount, 0) as total_amount, coalesce(t.order_count, 0) as order_count
from "shop"."marts"."customers" as c
left join __dbt__cte__int_order_totals as t on t.customer_id = c.customer_id