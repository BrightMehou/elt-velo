{{
  config(
    unique_key = ['id','created_date'],
  )
}}
select
   id,
   code,
   name,
   city_name,
   city_code,
   address,
   longitude,
   latitude,
   status,
   created_date,
   capacity,
from
     {{ ref('stg_station') }}

{% if is_incremental() %}

where created_date >= (select coalesce(max(created_date),'1900-01-01') from {{ this }} )

{% endif %}