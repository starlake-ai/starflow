{{ config(materialized='ephemeral') }}
select customer_id, sum(amount) as total_amount, count(*) as order_count
from {{ ref('stg_orders') }}
where amount >= {{ var('min_amount') }}
group by customer_id
