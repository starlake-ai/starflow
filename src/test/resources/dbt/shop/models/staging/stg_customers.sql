select
    id as customer_id,
    first_name,
    last_name
from {{ source('shop_raw', 'raw_customers') }}
