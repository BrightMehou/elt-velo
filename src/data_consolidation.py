import logging
import os
from datetime import datetime

from duckdb_tools import exec_sql_statments, get_city_code

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
today_date = datetime.now().strftime("%Y-%m-%d")


# Les code des villes
PARIS_CITY_CODE = 1
NANTES_CITY_CODE = 2
TOULOUSE_CITY_CODE = 3
STRASBOURG_CITY_CODE = 4

s3_endpoint = os.getenv("minio_endpoint", "localhost:9000")
s3_access_key = os.getenv("minio_access_key", "minioadmin")
s3_secret_key = os.getenv("minio_secret_key", "miniopassword")

s3_statement = f"""
INSTALL httpfs;
LOAD httpfs;
SET s3_url_style='path';
SET s3_use_ssl='false';
SET s3_endpoint='{s3_endpoint}';
SET s3_access_key_id='{s3_access_key}';
SET s3_secret_access_key='{s3_secret_key}';
SET s3_region='us-east-1';
"""

def consolidate_city_data():
    """Consolide les données des communes."""
    city_data_sql_statement = f"""
        {s3_statement}
        INSERT OR REPLACE into consolidate_city
        select 
            code as id,
            nom as name,
            population as nb_inhabitants,
            current_date() as created_date
        from read_json('s3://bicycle-data/{today_date}/commune_data.json')
    """
    exec_sql_statments(city_data_sql_statement)


def consolidate_paris_data():
    """Consolide les données de Paris."""
    paris_sql_statement = f"""
        {s3_statement}
        create temp table Paris AS 
            select * from read_json('s3://bicycle-data/{today_date}/paris_realtime_bicycle_data.json');

        insert or replace into consolidate_station 
        select 
            {PARIS_CITY_CODE} || '-' || stationcode as id,
            stationcode AS code,
            name,
            nom_arrondissement_communes as city_name,
            code_insee_commune as city_code,
            null as address,
            coordonnees_geo.lon as longitude,
            coordonnees_geo.lat as latitude,
            is_installed as status,
            current_date() as created_date,
            capacity
        from Paris;

        insert or replace into CONSOLIDATE_STATION_STATEMENT
        select 
            {PARIS_CITY_CODE} || '-' || stationcode as station_id,
            numdocksavailable AS bicycle_docks_available,
            numbikesavailable AS bicycle_available,
            duedate as last_statement_date,
            current_date() as created_date
        from Paris;
    """
    exec_sql_statments(paris_sql_statement)


def consolidate_nantes_toulouse():
    """Consolide les données pour Nantes et Toulouse."""
    cities = ["nantes", "toulouse"]
    cities_code = [NANTES_CITY_CODE, TOULOUSE_CITY_CODE]

    for city, city_code in zip(cities, cities_code):
        sql_statement = f"""
            {s3_statement}
            create temp table {city} AS 
                select * from read_json('s3://bicycle-data/{today_date}/{city}_realtime_bicycle_data.json');

            insert or replace into consolidate_station 
            select
                {city_code} || '-' || number as id,
                number as code,
                name,
                contract_name as city_name,
                {get_city_code(city)} as city_code,
                address,
                position.lon as longitude,
                position.lat as latitude,
                status,
                current_date() as created_date,
                bike_stands as capacity
            from {city};

            insert or replace into CONSOLIDATE_STATION_STATEMENT
            select
                {city_code} || '-' || number as station_id,
                available_bike_stands as bicycle_docks_available,
                available_bikes as bicycle_available,
                last_update as last_statement_date,
                current_date() as created_date
            from {city};
        """
        exec_sql_statments(sql_statement)


def consolidate_strasbourg_data():
    """Consolide les données de Strasbourg."""
    strasbourg_sql_statement = f"""
        {s3_statement}
        create temp table Strasbourg AS 
            select * from read_json('s3://bicycle-data/{today_date}/strasbourg_realtime_bicycle_data.json');

        insert or replace into consolidate_station
        select
            {STRASBOURG_CITY_CODE} || '-' || id as id,
            id as code,
            na as name,
            'strasbourg' as city_name,
            {get_city_code("strasbourg")} as city_code,
            null as address,
            lon as longitude,
            lat as latitude,
            is_installed as status,
            current_date() as created_date,
            "to" as capacity
        from Strasbourg;

        insert or replace into CONSOLIDATE_STATION_STATEMENT 
        select
            {STRASBOURG_CITY_CODE} || '-' || id as station_id,
            num_docks_available as bicycle_docks_available,
            av as bicycle_available,
            to_timestamp(last_reported::int) as last_statement_date,
            current_date() as created_date
        from Strasbourg;
    """
    exec_sql_statments(strasbourg_sql_statement)


def data_consolidation() -> None:
    """Orchestre la consolidation des données de toutes les villes."""
    consolidate_city_data()
    consolidate_paris_data()
    consolidate_nantes_toulouse()
    consolidate_strasbourg_data()
