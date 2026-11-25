
select customer_id, max(occurred_at) as last_event_at, count(*) as event_count
from "shop"."staging"."stg_events"

group by customer_id