-- Paris
select
    '{{ var("PARIS_CITY_CODE", "1") }}' || '-' || (json ->> 'stationcode') as id,
    json ->> 'stationcode' as code,
    json ->> 'name' as name,
    lower(json ->> 'nom_arrondissement_communes') as city_name,
    json ->> 'code_insee_commune' as city_code,
    null as address,
    (json -> 'coordonnees_geo' ->> 'lon')::DOUBLE as longitude,
    (json -> 'coordonnees_geo' ->> 'lat')::DOUBLE as latitude,
    json ->> 'is_installed' as status,
    current_date() as created_date,
    (json ->> 'capacity')::INTEGER as capacity
from read_json_objects_auto(
    's3://{{ var("BUCKET_NAME") }}/{{ run_started_at.strftime("%Y-%m-%d") }}/paris_realtime_bicycle_data.json'
)

union all

-- Nantes
select
    '{{ var("NANTES_CITY_CODE", "2") }}' || '-' || (json ->> 'number') as id,
    json ->> 'number' as code,
    json ->> 'name' as name,
    json ->> 'contract_name' as city_name,
    null as city_code,
    json ->> 'address' as address,
    (json -> 'position' ->> 'lon')::DOUBLE as longitude,
    (json -> 'position' ->> 'lat')::DOUBLE as latitude,
    json ->> 'status' as status,
    current_date() as created_date,
    (json ->> 'bike_stands')::INTEGER as capacity
from read_json_objects_auto(
    's3://{{ var("BUCKET_NAME") }}/{{ run_started_at.strftime("%Y-%m-%d") }}/nantes_realtime_bicycle_data.json'
)

union all

-- Toulouse
select
    '{{ var("TOULOUSE_CITY_CODE", "3") }}' || '-' || (json ->> 'number') as id,
    json ->> 'number' as code,
    json ->> 'name' as name,
    json ->> 'contract_name' as city_name,
    null as city_code,
    json ->> 'address' as address,
    (json -> 'position' ->> 'lon')::DOUBLE as longitude,
    (json -> 'position' ->> 'lat')::DOUBLE as latitude,
    json ->> 'status' as status,
    current_date() as created_date,
    (json ->> 'bike_stands')::INTEGER as capacity
from read_json_objects_auto(
    's3://{{ var("BUCKET_NAME") }}/{{ run_started_at.strftime("%Y-%m-%d") }}/toulouse_realtime_bicycle_data.json'
)

union all

-- Strasbourg
select
    '{{ var("STRASBOURG_CITY_CODE", "4") }}' || '-' || (json ->> 'id') as id,
    json ->> 'id' as code,
    json ->> 'na' as name,
    'strasbourg' as city_name,
    null as city_code,
    null as address,
    (json ->> 'lon')::DOUBLE as longitude,
    (json ->> 'lat')::DOUBLE as latitude,
    json ->> 'is_installed' as status,
    current_date() as created_date,
    (json ->> 'to')::INTEGER as capacity
from read_json_objects_auto(
    's3://{{ var("BUCKET_NAME") }}/{{ run_started_at.strftime("%Y-%m-%d") }}/strasbourg_realtime_bicycle_data.json'
)
