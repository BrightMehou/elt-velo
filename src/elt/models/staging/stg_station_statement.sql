select
    '{{ var("PARIS_CITY_CODE", "1") }}' || '-' || stationcode as id,
    numdocksavailable AS bicycle_docks_available,
    numbikesavailable AS bicycle_available,
    duedate as last_statement_date,
    current_date() as created_date,
from
    read_parquet(
        's3://bicycle-data/{{ run_started_at.strftime("%Y-%m-%d") }}/paris_realtime_bicycle_data.parquet'
    )
union
all -- Nantes avec jointure city_code
select
    '{{ var("NANTES_CITY_CODE", "2") }}' || '-'  || number as id,
    available_bike_stands as bicycle_docks_available,
    available_bikes as bicycle_available,
    last_update as last_statement_date,
    current_date() as created_date
from
    read_parquet(
        's3://bicycle-data/{{ run_started_at.strftime("%Y-%m-%d") }}/nantes_realtime_bicycle_data.parquet'
    ) 
union
all -- Toulouse avec jointure city_code
select
    '{{ var("TOULOUSE_CITY_CODE", "3") }}' || '-'  || number as id,
    available_bike_stands as bicycle_docks_available,
    available_bikes as bicycle_available,
    last_update as last_statement_date,
    current_date() as created_date
from
    read_parquet(
        's3://bicycle-data/{{ run_started_at.strftime("%Y-%m-%d") }}/toulouse_realtime_bicycle_data.parquet'
    ) 
union
all -- Strasbourg avec jointure city_code
select
    '{{ var("STRASBOURG_CITY_CODE", "4") }}' || '-'  || id as id,
    num_docks_available as bicycle_docks_available,
    av as bicycle_available,
    to_timestamp(last_reported::int) as last_statement_date,
    current_date() as created_date
from
    read_parquet('s3://bicycle-data/{{ run_started_at.strftime("%Y-%m-%d") }}/strasbourg_realtime_bicycle_data.parquet')