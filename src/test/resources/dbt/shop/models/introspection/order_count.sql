-- depends_on: {{ ref('stg_orders') }}
{% call statement('order_count', fetch_result=True) %}
    select count(*) as n from {{ ref('stg_orders') }}
{% endcall %}
{% set n = load_result('order_count')['data'][0][0] if execute else 0 %}
select {{ n }} as order_count
