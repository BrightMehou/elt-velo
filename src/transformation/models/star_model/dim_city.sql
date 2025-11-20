{{
  config(
    unique_key = 'id',
  )
}}
select
    id,
    name,
    nb_inhabitants
from {{ ref('consolidate_city') }}
{% if is_incremental() %}
WHERE created_date = (select MAX(created_date) from {{ ref('consolidate_city') }})
{% endif %}