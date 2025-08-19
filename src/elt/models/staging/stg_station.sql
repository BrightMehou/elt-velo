with city_codes_cte as (
        select
            id as insee_code,
            name as city_name,
            created_date
        from
            {{ ref('consolidate_city') }}
        where
            created_date = (
                select
                    max(created_date)
                from
                    {{ ref('consolidate_city') }}
            )
    ) -- Paris sans jointure car déjà code_insee_commune dans JSON
select
    '{{ var("PARIS_CITY_CODE", "1") }}' || '-' || stationcode as id,
    stationcode as code,
    name,
    lower(nom_arrondissement_communes) as city_name,
    code_insee_commune as city_code,
    null as address,
    coordonnees_geo.lon as longitude,
    coordonnees_geo.lat as latitude,
    is_installed as status,
    current_date() as created_date,
    capacity
from
    read_json(
        's3://bicycle-data/{{ run_started_at.strftime("%Y-%m-%d") }}/paris_realtime_bicycle_data.json'
    )
union
all -- Nantes avec jointure city_code
select
    '{{ var("NANTES_CITY_CODE", "2") }}' || '-'  || source.number as id,
    source.number as code,
    source.name,
    source.contract_name as city_name,
    c.insee_code as city_code,
    source.address,
    source.position.lon as longitude,
    source.position.lat as latitude,
    source.status,
    current_date() as created_date,
    source.bike_stands as capacity
from
    read_json(
        's3://bicycle-data/{{ run_started_at.strftime("%Y-%m-%d") }}/nantes_realtime_bicycle_data.json'
    ) as source
    left join city_codes_cte c on source.contract_name = c.city_name
union
all -- Toulouse avec jointure city_code
select
    '{{ var("TOULOUSE_CITY_CODE", "3") }}' || '-'  || source.number as id,
    source.number as code,
    source.name,
    source.contract_name as city_name,
    c.insee_code as city_code,
    source.address,
    source.position.lon as longitude,
    source.position.lat as latitude,
    source.status,
    current_date() as created_date,
    source.bike_stands as capacity
from
    read_json(
        's3://bicycle-data/{{ run_started_at.strftime("%Y-%m-%d") }}/toulouse_realtime_bicycle_data.json'
    ) as source
    left join city_codes_cte c on source.contract_name = c.city_name
union
all -- Strasbourg avec jointure city_code
select
    '{{ var("STRASBOURG_CITY_CODE", "4") }}' || '-'  || source.id as id,
    source.id as code,
    source.na as name,
    'strasbourg' as city_name,
    c.insee_code as city_code,
    null as address,
    source.lon as longitude,
    source.lat as latitude,
    source.is_installed as status,
    current_date() as created_date,
    source."to" as capacity
from
    read_json('s3://bicycle-data/{{ run_started_at.strftime("%Y-%m-%d") }}/strasbourg_realtime_bicycle_data.json') as source
left join city_codes_cte c on c.city_name = 'strasbourg'