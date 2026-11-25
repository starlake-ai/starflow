{% if execute %}{% do run_query("create table if not exists marts.sl_import_side_effect as select 1 as a") %}{% endif %}
select 1 as a
