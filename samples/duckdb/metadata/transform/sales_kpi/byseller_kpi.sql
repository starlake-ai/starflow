with mycte as (
    select o.amount, c.id, CURRENT_TIMESTAMP as timestamp
    from sales.orders o, sales.customers c
    where o.customer_id = c.id
)
select id, sum(amount) as sum, timestamp,
       [{'city': 'Seattle', 'state': 'Washington'}, {'city': 'Phoenix', 'state': 'Arizona'}] AS location1,
       {'city': 'Seattle', 'state': 'Washington'} AS location2
from mycte
group by mycte.id, mycte.timestamp
