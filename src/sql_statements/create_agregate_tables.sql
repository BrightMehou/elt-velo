create table if not exists dim_station (
    id varchar primary key,
    code varchar,
    name varchar,
    address varchar,
    longitude float,
    latitude float,
    status varchar,
    capacitty integer
);

create table if not exists dim_city (
    id varchar primary key,
    name varchar,
    nb_inhabitants integer
);

create table if not exists fact_station_statement (
    station_id varchar not null,
    city_id varchar not null,
    bicycle_docks_available integer,
    bicycle_available integer,
    last_statement_date datetime,
    created_date date default current_date,
    primary key (station_id, city_id, created_date),
    foreign key (station_id) references dim_station (id),
    foreign key (city_id) references dim_city (id)
);
