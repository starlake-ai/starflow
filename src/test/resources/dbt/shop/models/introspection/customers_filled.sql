{% set wanted = [
    {'name': 'id', 'datatype': 'integer'},
    {'name': 'first_name', 'datatype': 'varchar'},
    {'name': 'email', 'datatype': 'varchar'}
] %}
select
    {{ fill_staging_columns(adapter.get_columns_in_relation(source('shop_raw', 'raw_customers')), wanted) }}
from {{ source('shop_raw', 'raw_customers') }}
