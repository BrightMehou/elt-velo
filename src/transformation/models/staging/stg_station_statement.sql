-- Paris
select
    '{{ var("PARIS_CITY_CODE", "1") }}' || '-' || (json ->> 'stationcode') as id,
    (json ->> 'numdocksavailable')::INTEGER as bicycle_docks_available,
    (json ->> 'numbikesavailable')::INTEGER as bicycle_available,
    (json ->> 'duedate')::TIMESTAMP as last_statement_date,
    current_date() as created_date
from read_json_objects_auto(
    's3://{{ var("BUCKET_NAME") }}/{{ run_started_at.strftime("%Y-%m-%d") }}/paris_realtime_bicycle_data.json'
)

union all

-- Nantes
select
    '{{ var("NANTES_CITY_CODE", "2") }}' || '-' || (json ->> 'number') as id,
    (json ->> 'available_bike_stands')::INTEGER as bicycle_docks_available,
    (json ->> 'available_bikes')::INTEGER as bicycle_available,
    (json ->> 'last_update')::TIMESTAMP as last_statement_date,
    current_date() as created_date
from read_json_objects_auto(
    's3://{{ var("BUCKET_NAME") }}/{{ run_started_at.strftime("%Y-%m-%d") }}/nantes_realtime_bicycle_data.json'
)

union all

-- Toulouse
select
    '{{ var("TOULOUSE_CITY_CODE", "3") }}' || '-' || (json ->> 'number') as id,
    (json ->> 'available_bike_stands')::INTEGER as bicycle_docks_available,
    (json ->> 'available_bikes')::INTEGER as bicycle_available,
    (json ->> 'last_update')::TIMESTAMP as last_statement_date,
    current_date() as created_date
from read_json_objects_auto(
    's3://{{ var("BUCKET_NAME") }}/{{ run_started_at.strftime("%Y-%m-%d") }}/toulouse_realtime_bicycle_data.json'
)

union all

-- Strasbourg
select
    '{{ var("STRASBOURG_CITY_CODE", "4") }}' || '-' || (json ->> 'id') as id,
    (json ->> 'num_docks_available')::INTEGER as bicycle_docks_available,
    (json ->> 'av')::INTEGER as bicycle_available,
    to_timestamp((json ->> 'last_reported')::INT) as last_statement_date,
    current_date() as created_date
from read_json_objects_auto(
    's3://{{ var("BUCKET_NAME") }}/{{ run_started_at.strftime("%Y-%m-%d") }}/strasbourg_realtime_bicycle_data.json'
)
