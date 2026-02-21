CREATE TABLE IF NOT EXISTS consolidate_station (
    id varchar NOT NULL,
    code varchar NOT NULL,
    name varchar,
    city_name varchar,
    city_code varchar,
    address varchar,
    longitude float,
    latitude float,
    STATUS varchar,
    created_date date,
    capacity integer,
    PRIMARY KEY (id, created_date)
);

CREATE TABLE IF NOT EXISTS consolidate_city (
    id varchar,
    name varchar,
    nb_inhabitants integer,
    created_date date,
    PRIMARY KEY (id, created_date)
);

CREATE TABLE IF NOT EXISTS consolidate_station_statement (
    station_id varchar NOT NULL,
    bicycle_docks_available integer,
    bicycle_available integer,
    last_statement_date timestamp,
    created_date date,
    PRIMARY KEY (station_id, created_date)
);