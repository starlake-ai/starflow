select c.customer_id, coalesce(t.total_amount, 0) as total_amount, coalesce(t.order_count, 0) as order_count
from {{ ref('customers') }} as c
left join {{ ref('int_order_totals') }} as t on t.customer_id = c.customer_id
