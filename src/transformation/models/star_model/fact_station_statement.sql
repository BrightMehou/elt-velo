{{
  config(
    unique_key = 'station_id',
  )
}}
with temp as ( select 
    station_id ,
    bicycle_docks_available ,
    bicycle_available,
    last_statement_date,
from {{ ref('consolidate_station_statement') }}
{% if is_incremental() %}
where created_date = (select max(created_date) from {{ ref('consolidate_station_statement') }})
{% endif %}
)

select
    temp.station_id,
    city.id as city_id,
    temp.bicycle_docks_available,
    temp.bicycle_available,
    temp.last_statement_date
from temp
join {{ ref('consolidate_station') }} as station
on temp.station_id = station.id
left join {{ ref('dim_city') }} as city
on station.city_code = city.id