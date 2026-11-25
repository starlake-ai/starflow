with mycte as (
    select o.amount, c.id, CURRENT_TIMESTAMP as timestamp
    from sales.orders o, sales.customers c
    where o.customer_id = c.id
)
select id, sum(amount) as sum, timestamp
from mycte
group by mycte.id, mycte.timestamp



