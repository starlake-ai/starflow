
select order_id, customer_id, order_date, order_month, status, amount, 'web' as channel
from "shop"."staging"."stg_orders"