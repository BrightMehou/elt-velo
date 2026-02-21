SELECT
    dm.name,
    tmp.sum_bicycle_docks_available
FROM
    {{ ref('dim_city') }} AS dm
    INNER JOIN (
        SELECT
            city_id,
            SUM(bicycle_docks_available) AS sum_bicycle_docks_available
        FROM
            {{ ref('fact_station_statement') }}
        GROUP BY
            city_id
    ) AS tmp ON dm.id = tmp.city_id
WHERE
    LOWER(dm.name) IN (
        'paris',
        'nantes',
        'strasbourg',
        'toulouse'
    )