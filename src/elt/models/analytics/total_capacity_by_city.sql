select 
    dc.name as city_name,
    sum(ds.capacity) as total_capacity
from {{ ref('dim_station') }} ds
join {{ ref('fact_station_statement') }} fss 
    on ds.id = fss.station_id
join {{ ref('dim_city') }} dc 
    on fss.city_id = dc.id
group by dc.name