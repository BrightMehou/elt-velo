select 
    dm.name,
    tmp.sum_bicycle_docks_available
from {{ ref('dim_city') }} dm
inner join (
    select 
        city_id, 
        sum(bicycle_docks_available) as sum_bicycle_docks_available
    from {{ ref('fact_station_statement') }}
    group by city_id
) tmp on dm.id = tmp.city_id
where lower(dm.name) in ('paris', 'nantes', 'strasbourg', 'toulouse')