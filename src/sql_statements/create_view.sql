create
or replace view map_station as
select
    ds.name,
    ds.latitude,
    ds.longitude,
    fss.bicycle_available,
    ds.capacitty,
    fss.created_date
from
    dim_station ds
    join fact_station_statement fss on ds.id = fss.station_id
where
    fss.created_date = (
        select
            max(created_date)
        from
            fact_station_statement
        where
            station_id = ds.id
    )
    and ds.latitude is not null
    and ds.longitude is not null;

create
or replace view available_emplacement_by_city as
select
    dm.name,
    tmp.sum_bicycle_docks_available
from
    dim_city dm
    inner join (
        select
            city_id,
            sum(bicycle_docks_available) as sum_bicycle_docks_available
        from
            fact_station_statement
        where
            created_date = (
                select
                    max(created_date)
                from
                    consolidate_station
            )
        group by
            city_id
    ) tmp on dm.id = tmp.city_id
where
    lower(dm.name) in ('paris', 'nantes', 'strasbourg', 'toulouse');

create
or replace view mean_bicycle_available_by_station as
select
    ds.name,
    ds.code,
    ds.address,
    tmp.avg_dock_available
from
    dim_station ds
    join (
        select
            station_id,
            avg(bicycle_available) as avg_dock_available
        from
            fact_station_statement
        group by
            station_id
    ) tmp on ds.id = tmp.station_id;

create
or replace view total_capacity_by_city as
select
    dc.name as city_name,
    sum(ds.capacitty) as total_capacity
from
    dim_station ds
    join fact_station_statement fss on ds.id = fss.station_id
    join dim_city dc on fss.city_id = dc.id
group by
    dc.name;