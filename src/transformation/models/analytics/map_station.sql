select 
    ds.id,
    ds.code,
    ds.name,
    ds.address,
    ds.latitude,
    ds.longitude,
    ds.status,
    ds.capacity,
    fss.bicycle_docks_available,
    fss.bicycle_available,
    fss.last_statement_date
from {{ ref('dim_station') }} ds
join {{ ref('fact_station_statement') }} fss 
    on ds.id = fss.station_id
where 
    ds.latitude is not null
    and ds.longitude is not null