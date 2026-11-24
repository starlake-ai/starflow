select
    id as order_id,
    customer_id,
    cast(order_date as date) as order_date,
    {{ dbt.date_trunc('month', 'cast(order_date as date)') }} as order_month,
    status,
    {{ cents_to_dollars('amount_cents') }} as amount,
    '{{ var("currency") }}' as currency
from {{ ref('raw_orders') }}
