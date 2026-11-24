select
    id as order_id,
    customer_id,
    cast(order_date as date) as order_date,
    date_trunc('month', cast(order_date as date)) as order_month,
    status,
    (amount_cents / 100)::numeric(16, 2) as amount,
    'EUR' as currency
from "shop"."raw"."raw_orders"