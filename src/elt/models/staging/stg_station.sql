select
    '{{ var("PARIS_CITY_CODE", "1") }}' || '-' || stationcode as id,
    stationcode as code,
    name,
    lower(nom_arrondissement_communes) as city_name,
    code_insee_commune as city_code,
    null as address,
    coordonnees_geo.lon as longitude,
    coordonnees_geo.lat as latitude,
    is_installed as status,
    current_date() as created_date,
    capacity
from
    read_json(
        's3://bicycle-data/{{ run_started_at.strftime("%Y-%m-%d") }}/paris_realtime_bicycle_data.json'
    )
union
all 
select
    '{{ var("NANTES_CITY_CODE", "2") }}' || '-'  || number as id,
    number as code,
    name,
    contract_name as city_name,
    null as city_code,
    address,
    position.lon as longitude,
    position.lat as latitude,
    status,
    current_date() as created_date,
    bike_stands as capacity
from
    read_json(
        's3://bicycle-data/{{ run_started_at.strftime("%Y-%m-%d") }}/nantes_realtime_bicycle_data.json'
    ) 
union
all 
select
    '{{ var("TOULOUSE_CITY_CODE", "3") }}' || '-'  || number as id,
    number as code,
    name,
    contract_name as city_name,
    null as city_code,
    address,
    position.lon as longitude,
    position.lat as latitude,
    status,
    current_date() as created_date,
    bike_stands as capacity
from
    read_json(
        's3://bicycle-data/{{ run_started_at.strftime("%Y-%m-%d") }}/toulouse_realtime_bicycle_data.json'
    ) 
union
all 
select
    '{{ var("STRASBOURG_CITY_CODE", "4") }}' || '-'  || id as id,
    id as code,
    na as name,
    'strasbourg' as city_name,
    null as city_code,
    null as address,
    lon as longitude,
    lat as latitude,
    is_installed as status,
    current_date() as created_date,
    "to" as capacity
from
    read_json('s3://bicycle-data/{{ run_started_at.strftime("%Y-%m-%d") }}/strasbourg_realtime_bicycle_data.json') 