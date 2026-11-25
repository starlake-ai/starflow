{% set answer = run_query("select 42 as answer").columns[0].values()[0] if execute else 0 %}
select {{ answer }} as answer
