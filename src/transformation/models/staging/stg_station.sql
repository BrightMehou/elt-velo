-- Paris
SELECT
    '{{ var("PARIS_CITY_CODE", "1") }}' || '-' || (json ->> 'stationcode') AS id,
    json ->> 'stationcode' AS code,
    json ->> 'name' AS name,
    lower(json ->> 'nom_arrondissement_communes') AS city_name,
    json ->> 'code_insee_commune' AS city_code,
    NULL AS address,
    (json -> 'coordonnees_geo' ->> 'lon') :: DOUBLE PRECISION AS longitude,
    (json -> 'coordonnees_geo' ->> 'lat') :: DOUBLE PRECISION AS latitude,
    json ->> 'is_installed' AS STATUS,
    current_date AS created_date,
    (json ->> 'capacity') :: INTEGER AS capacity
FROM
    staging_raw,
    jsonb_array_elements(data) AS json
WHERE
    nom = 'paris_realtime_bicycle_data.json'
    AND date = current_date
UNION
ALL -- Nantes
SELECT
    '{{ var("NANTES_CITY_CODE", "2") }}' || '-' || (json ->> 'number') AS id,
    json ->> 'number' AS code,
    json ->> 'name' AS name,
    json ->> 'contract_name' AS city_name,
    NULL AS city_code,
    json ->> 'address' AS address,
    (json -> 'position' ->> 'lon') :: DOUBLE PRECISION AS longitude,
    (json -> 'position' ->> 'lat') :: DOUBLE PRECISION AS latitude,
    json ->> 'status' AS STATUS,
    current_date AS created_date,
    (json ->> 'bike_stands') :: INTEGER AS capacity
FROM
    staging_raw,
    jsonb_array_elements(data) AS json
WHERE
    nom = 'nantes_realtime_bicycle_data.json'
    AND date = current_date
UNION
ALL -- Toulouse
SELECT
    '{{ var("TOULOUSE_CITY_CODE", "3") }}' || '-' || (json ->> 'number') AS id,
    json ->> 'number' AS code,
    json ->> 'name' AS name,
    json ->> 'contract_name' AS city_name,
    NULL AS city_code,
    json ->> 'address' AS address,
    (json -> 'position' ->> 'lon') :: DOUBLE PRECISION AS longitude,
    (json -> 'position' ->> 'lat') :: DOUBLE PRECISION AS latitude,
    json ->> 'status' AS STATUS,
    current_date AS created_date,
    (json ->> 'bike_stands') :: INTEGER AS capacity
FROM
    staging_raw,
    jsonb_array_elements(data) AS json
WHERE
    nom = 'toulouse_realtime_bicycle_data.json'
    AND date = current_date
UNION
ALL -- Strasbourg
SELECT
    '{{ var("STRASBOURG_CITY_CODE", "4") }}' || '-' || (json ->> 'id') AS id,
    json ->> 'id' AS code,
    json ->> 'na' AS name,
    'strasbourg' AS city_name,
    NULL AS city_code,
    NULL AS address,
    (json ->> 'lon') :: DOUBLE PRECISION AS longitude,
    (json ->> 'lat') :: DOUBLE PRECISION AS latitude,
    json ->> 'is_installed' AS STATUS,
    current_date AS created_date,
    (json ->> 'to') :: INTEGER AS capacity
FROM
    staging_raw,
    jsonb_array_elements(data) AS json
WHERE
    nom = 'strasbourg_realtime_bicycle_data.json'
    AND date = current_date