-- Paris
select
    '{{ var("PARIS_CITY_CODE", "1") }}' || '-' || json['stationcode']::TEXT as id,
    json['stationcode']::TEXT as code,
    json['name']::TEXT as name,
    lower(json['nom_arrondissement_communes']::TEXT) as city_name,
    json['code_insee_commune']::TEXT as city_code,
    null as address,
    (json['coordonnees_geo']['lon'])::DOUBLE as longitude,
    (json['coordonnees_geo']['lat'])::DOUBLE as latitude,
    json['is_installed']::TEXT as status,
    current_date() as created_date,
    json['capacity']::INTEGER as capacity
from read_json_objects_auto(
    's3://bicycle-data/{{ run_started_at.strftime("%Y-%m-%d") }}/paris_realtime_bicycle_data.json'
)

union all

-- Nantes
select
    '{{ var("NANTES_CITY_CODE", "2") }}' || '-' || json['number']::TEXT as id,
    json['number']::TEXT as code,
    json['name']::TEXT as name,
    json['contract_name']::TEXT as city_name,
    null as city_code,
    json['address']::TEXT as address,
    (json['position']['lon'])::DOUBLE as longitude,
    (json['position']['lat'])::DOUBLE as latitude,
    json['status']::TEXT as status,
    current_date() as created_date,
    json['bike_stands']::INTEGER as capacity
from read_json_objects_auto(
    's3://bicycle-data/{{ run_started_at.strftime("%Y-%m-%d") }}/nantes_realtime_bicycle_data.json'
)

union all

-- Toulouse
select
    '{{ var("TOULOUSE_CITY_CODE", "3") }}' || '-' || json['number']::TEXT as id,
    json['number']::TEXT as code,
    json['name']::TEXT as name,
    json['contract_name']::TEXT as city_name,
    null as city_code,
    json['address']::TEXT as address,
    (json['position']['lon'])::DOUBLE as longitude,
    (json['position']['lat'])::DOUBLE as latitude,
    json['status']::TEXT as status,
    current_date() as created_date,
    json['bike_stands']::INTEGER as capacity
from read_json_objects_auto(
    's3://bicycle-data/{{ run_started_at.strftime("%Y-%m-%d") }}/toulouse_realtime_bicycle_data.json'
)

union all

-- Strasbourg
select
    '{{ var("STRASBOURG_CITY_CODE", "4") }}' || '-' || json['id']::TEXT as id,
    json['id']::TEXT as code,
    json['na']::TEXT as name,
    'strasbourg' as city_name,
    null as city_code,
    null as address,
    json['lon']::DOUBLE as longitude,
    json['lat']::DOUBLE as latitude,
    json['is_installed']::TEXT as status,
    current_date() as created_date,
    json['to']::INTEGER as capacity
from read_json_objects_auto(
    's3://bicycle-data/{{ run_started_at.strftime("%Y-%m-%d") }}/strasbourg_realtime_bicycle_data.json'
)
