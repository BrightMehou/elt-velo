SELECT
    ds.name,
    ds.code,
    ds.address,
    tmp.avg_dock_available
FROM
    {{ ref('dim_station') }} AS ds
    INNER JOIN (
        SELECT
            station_id,
            AVG(bicycle_available) AS avg_dock_available
        FROM
            {{ ref('fact_station_statement') }}
        GROUP BY
            station_id
    ) AS tmp ON ds.id = tmp.station_id