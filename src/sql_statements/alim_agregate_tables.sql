insert
    or replace into dim_station
select
    id,
    code,
    name,
    address,
    longitude,
    latitude,
    status,
    capacity
from
    consolidate_station
where
    created_date = (
        select
            max(created_date)
        from
            consolidate_station
    );

insert
    or replace into dim_city
select
    id,
    name,
    nb_inhabitants
from
    consolidate_city
where
    created_date = (
        select
            max(created_date)
        from
            consolidate_city
    );

insert
    or replace into fact_station_statement
select
    station_id,
    cc.id as city_id,
    bicycle_docks_available,
    bicycle_available,
    last_statement_date,
    current_date as created_date
from
    consolidate_station_statement
    join consolidate_station on consolidate_station.id = consolidate_station_statement.station_id
    left join consolidate_city as cc on cc.id = consolidate_station.city_code
where
    city_code != 0
    and consolidate_station_statement.created_date = (
        select
            max(created_date)
        from
            consolidate_station_statement
    )
    and consolidate_station.created_date = (
        select
            max(created_date)
        from
            consolidate_station
    )
    and cc.created_date = (
        select
            max(created_date)
        from
            consolidate_city
    );