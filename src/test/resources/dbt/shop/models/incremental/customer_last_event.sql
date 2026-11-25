{{ config(incremental_strategy='merge', unique_key='customer_id') }}
select customer_id, max(occurred_at) as last_event_at, count(*) as event_count
from {{ ref('stg_events') }}
{% if is_incremental() %}
where customer_id in (select customer_id from {{ ref('stg_events') }} where occurred_at > (select max(last_event_at) from {{ this }}))
{% endif %}
group by customer_id
