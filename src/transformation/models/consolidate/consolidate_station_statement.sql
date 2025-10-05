{{
  config(
    unique_key = ['station_id', 'created_date'],
  )
}}
select
   id as station_id,
   bicycle_docks_available,
   bicycle_available,
   last_statement_date::timestamp as last_statement_date,
   created_date
from
     {{ ref('stg_station_statement') }}

{% if is_incremental() %}

where created_date >= (select coalesce(max(created_date),'1900-01-01') from {{ this }} )

{% endif %}