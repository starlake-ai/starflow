select event_id, customer_id, event_type, cast(occurred_at as timestamp) as occurred_at
from "shop"."raw"."raw_events"