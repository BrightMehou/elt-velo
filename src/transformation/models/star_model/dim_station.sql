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
where created_date = (select max(created_date) from {{ ref('consolidate_station') }})