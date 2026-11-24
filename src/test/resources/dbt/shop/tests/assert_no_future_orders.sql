select * from {{ ref('orders') }} where order_date > current_date
