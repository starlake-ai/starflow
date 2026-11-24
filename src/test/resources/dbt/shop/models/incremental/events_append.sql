{{ config(incremental_strategy='append') }}
select event_id, customer_id, event_type, occurred_at
from {{ ref('stg_events') }}
{% if is_incremental() %}
where occurred_at > (select max(occurred_at) from {{ this }})
{% endif %}
