CREATE
OR REPLACE VIEW MAP_STATION AS
SELECT
    ds.NAME,
    ds.LATITUDE,
    ds.LONGITUDE,
    fss.BICYCLE_AVAILABLE,
    ds.CAPACITTY,
    fss.CREATED_DATE
FROM
    DIM_STATION ds
    JOIN FACT_STATION_STATEMENT fss ON ds.ID = fss.STATION_ID
WHERE
    fss.CREATED_DATE = (
        SELECT
            MAX(CREATED_DATE)
        FROM
            FACT_STATION_STATEMENT
        WHERE
            STATION_ID = ds.ID
    )
    AND ds.LATITUDE IS NOT NULL
    AND ds.LONGITUDE IS NOT NULL;

CREATE
OR REPLACE VIEW AVAILABLE_EMPLACEMENT_BY_CITY AS
SELECT
    dm.NAME,
    tmp.SUM_BICYCLE_DOCKS_AVAILABLE
FROM
    DIM_CITY dm
    INNER JOIN (
        SELECT
            CITY_ID,
            SUM(BICYCLE_DOCKS_AVAILABLE) AS SUM_BICYCLE_DOCKS_AVAILABLE
        FROM
            FACT_STATION_STATEMENT
        WHERE
            CREATED_DATE = (
                SELECT
                    MAX(CREATED_DATE)
                FROM
                    CONSOLIDATE_STATION
            )
        GROUP BY
            CITY_ID
    ) tmp ON dm.ID = tmp.CITY_ID
WHERE
    lower(dm.NAME) IN ('paris', 'nantes', 'strasbourg', 'toulouse');

CREATE
OR REPLACE VIEW MEAN_BICYCLE_AVAILABLE_BY_STATION AS
SELECT
    ds.NAME,
    ds.CODE,
    ds.ADDRESS,
    tmp.AVG_DOCK_AVAILABLE
FROM
    DIM_STATION ds
    JOIN (
        SELECT
            STATION_ID,
            AVG(BICYCLE_AVAILABLE) AS AVG_DOCK_AVAILABLE
        FROM
            FACT_STATION_STATEMENT
        GROUP BY
            STATION_ID
    ) tmp ON ds.ID = tmp.STATION_ID;

CREATE
OR REPLACE VIEW TOTAL_CAPACITY_BY_CITY AS
SELECT
    dc.name AS city_name,
    SUM(ds.CAPACITTY) AS total_capacity
FROM
    DIM_STATION ds
    JOIN FACT_STATION_STATEMENT fss ON ds.ID = fss.STATION_ID
    JOIN DIM_CITY dc ON fss.CITY_ID = dc.ID
GROUP BY
    dc.name;