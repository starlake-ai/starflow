{% macro fill_staging_columns(source_columns, staging_columns) %}
{%- set source_names = source_columns | map(attribute='name') | map('lower') | list -%}
{%- for column in staging_columns %}
    {% if column.name | lower in source_names -%}
        {{ column.name }}
    {%- else -%}
        cast(null as {{ column.datatype }}) as {{ column.name }}
    {%- endif -%}
    {%- if not loop.last %},{% endif -%}
{% endfor %}
{% endmacro %}
