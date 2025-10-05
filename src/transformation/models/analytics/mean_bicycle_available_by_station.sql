select 
    ds.name,
    ds.code,
    ds.address,
    tmp.avg_dock_available
from {{ ref('dim_station') }} ds
join (
    select 
        station_id, 
        avg(bicycle_available) as avg_dock_available
    from {{ ref('fact_station_statement') }}
    group by station_id
) tmp on ds.id = tmp.station_id