{{ config(incremental_strategy='delete+insert', unique_key=['event_date', 'event_type']) }}
select cast(occurred_at as date) as event_date, event_type, count(*) as events
from {{ ref('stg_events') }}
{% if is_incremental() %}
where cast(occurred_at as date) >= (select max(event_date) from {{ this }})
{% endif %}
group by 1, 2
