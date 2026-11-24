select
    md5(cast(coalesce(cast(customer_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(last_name as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as customer_key,
    customer_id,
    first_name,
    last_name
from "shop"."staging"."stg_customers"