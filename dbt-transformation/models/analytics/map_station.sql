SELECT
    ds.id,
    ds.code,
    ds.name,
    ds.address,
    ds.latitude,
    ds.longitude,
    ds.status,
    ds.capacity,
    fss.bicycle_docks_available,
    fss.bicycle_available,
    fss.last_statement_date
FROM
    {{ ref('dim_station') }} AS ds
    INNER JOIN {{ ref('fact_station_statement') }} AS fss ON ds.id = fss.station_id
WHERE
    ds.latitude IS NOT NULL
    AND ds.longitude IS NOT NULL