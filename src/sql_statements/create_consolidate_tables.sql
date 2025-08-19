create table if not exists consolidate_station  (
    id varchar not null,
    code varchar not null,
    name varchar,
    city_name varchar,
    city_code varchar,
    address varchar,
    longitude float,
    latitude float,
    status varchar,
    created_date date,
    capacity integer,
    primary key (id, created_date)
);

create table if not exists consolidate_city (
    id varchar,
    name varchar,
    nb_inhabitants integer,
    created_date date,
    primary key (id, created_date)
);

create table if not exists consolidate_station_statement (
    station_id varchar not null,
    bicycle_docks_available integer,
    bicycle_available integer,
    last_statement_date timestamp,
    created_date date,
    primary key (station_id, created_date)
);
