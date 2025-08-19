{{
  config(
    unique_key = ['id', 'created_date'],
  )
}}
select
   id,
   name,
   nb_inhabitants,
   created_date::date as created_date
from
     {{ ref('stg_city') }}

{% if is_incremental() %}

where created_date >= (select coalesce(max(created_date),'1900-01-01') from {{ this }} )

{% endif %}