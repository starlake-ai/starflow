{{ config(
    pre_hook="create table if not exists {{ this.schema }}.audit_log (model_name varchar)",
    post_hook="insert into {{ this.schema }}.audit_log values ('{{ this.name }}')",
    tags=['finance']
) }}
select order_id, customer_id, order_date, order_month, status, amount, '{{ env_var("SHOP_CHANNEL", "web") }}' as channel
from {{ ref('stg_orders') }}
