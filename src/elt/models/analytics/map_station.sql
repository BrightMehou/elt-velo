select 
    ds.name,
    ds.latitude,
    ds.longitude,
    fss.bicycle_available,
    ds.capacity,
    fss.last_statement_date
from {{ ref('dim_station') }} ds
join {{ ref('fact_station_statement') }} fss 
    on ds.id = fss.station_id
where 
    ds.latitude is not null
    and ds.longitude is not null