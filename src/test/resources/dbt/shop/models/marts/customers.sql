select
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'last_name']) }} as customer_key,
    customer_id,
    first_name,
    last_name
from {{ ref('stg_customers') }}
