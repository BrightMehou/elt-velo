CREATE TABLE IF NOT EXISTS dim_station (
    id varchar PRIMARY KEY,
    code varchar,
    name varchar,
    address varchar,
    longitude float,
    latitude float,
    STATUS varchar,
    capacity integer
);

CREATE TABLE IF NOT EXISTS dim_city (
    id varchar PRIMARY KEY,
    name varchar,
    nb_inhabitants integer
);

CREATE TABLE IF NOT EXISTS fact_station_statement (
    station_id varchar NOT NULL,
    city_id varchar NOT NULL,
    bicycle_docks_available integer,
    bicycle_available integer,
    last_statement_date timestamp,
    created_date date DEFAULT current_date,
    PRIMARY KEY (station_id, city_id, created_date) -- foreign key (station_id) references dim_station (id),
    -- foreign key (city_id) references dim_city (id)
);