
-- Use the `ref` function to select from other models
-- depends_on: {{ ref('my_first_dbt_model') }}

select *
from {{ ref('my_first_dbt_model') }}
where id = 1
