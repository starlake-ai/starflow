
select cast(occurred_at as date) as event_date, event_type, count(*) as events
from "shop"."staging"."stg_events"

group by 1, 2