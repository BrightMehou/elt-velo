{{ config(unique_key = 'station_id',) }} WITH temp AS (
    SELECT
        station_id,
        bicycle_docks_available,
        bicycle_available,
        last_statement_date
    FROM
        {{ ref('consolidate_station_statement') }} {% if is_incremental() %}
    WHERE
        created_date = (
            SELECT
                max(created_date)
            FROM
                {{ ref('consolidate_station_statement') }}
        ) {% endif %}
)
SELECT
    temp.station_id,
    city.id AS city_id,
    temp.bicycle_docks_available,
    temp.bicycle_available,
    temp.last_statement_date
FROM
    temp
    INNER JOIN {{ ref('consolidate_station') }} AS station ON temp.station_id = station.id
    LEFT JOIN {{ ref('dim_city') }} AS city ON station.city_code = city.id