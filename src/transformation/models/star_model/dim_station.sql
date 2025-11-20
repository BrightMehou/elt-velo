{{
  config(
    unique_key = 'id',
  )
}}
select
    id,
    code,
    name,
    address,
    longitude,
    latitude,
    status,
    capacity,
from {{ ref('consolidate_station') }}
{% if is_incremental() %}
where created_date = (select max(created_date) from {{ ref('consolidate_station') }})
{% endif %}
