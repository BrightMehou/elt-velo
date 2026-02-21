SELECT
    dc.name AS city_name,
    sum(ds.capacity) AS total_capacity
FROM
    {{ ref('dim_station') }} AS ds
    JOIN {{ ref('fact_station_statement') }} AS fss ON ds.id = fss.station_id
    JOIN {{ ref('dim_city') }} AS dc ON fss.city_id = dc.id
GROUP BY
    dc.name