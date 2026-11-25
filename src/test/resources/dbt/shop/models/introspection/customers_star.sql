select {{ dbt_utils.star(from=ref('stg_customers'), except=['last_name']) }}
from {{ ref('stg_customers') }}
