-- Paris
SELECT
    '{{ var("PARIS_CITY_CODE", "1") }}' || '-' || (json ->> 'stationcode') AS id,
    (json ->> 'numdocksavailable') :: INTEGER AS bicycle_docks_available,
    (json ->> 'numbikesavailable') :: INTEGER AS bicycle_available,
    (json ->> 'duedate') :: TIMESTAMP AS last_statement_date,
    current_date AS created_date
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
    (json ->> 'available_bike_stands') :: INTEGER AS bicycle_docks_available,
    (json ->> 'available_bikes') :: INTEGER AS bicycle_available,
    (json ->> 'last_update') :: TIMESTAMP AS last_statement_date,
    current_date AS created_date
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
    (json ->> 'available_bike_stands') :: INTEGER AS bicycle_docks_available,
    (json ->> 'available_bikes') :: INTEGER AS bicycle_available,
    (json ->> 'last_update') :: TIMESTAMP AS last_statement_date,
    current_date AS created_date
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
    (json ->> 'num_docks_available') :: INTEGER AS bicycle_docks_available,
    (json ->> 'av') :: INTEGER AS bicycle_available,
    to_timestamp((json ->> 'last_reported') :: INT) AS last_statement_date,
    current_date AS created_date
FROM
    staging_raw,
    jsonb_array_elements(data) AS json
WHERE
    nom = 'strasbourg_realtime_bicycle_data.json'
    AND date = current_date