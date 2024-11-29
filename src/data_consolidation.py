import json
from datetime import datetime, date

import duckdb
import pandas as pd

today_date = datetime.now().strftime("%Y-%m-%d")
PARIS_CITY_CODE = 1
NANTES_CITY_CODE = 2
TOULOUSE_CITY_CODE = 3

def create_consolidate_tables() -> None:
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    with open("data/sql_statements/create_consolidate_tables.sql") as fd:
        statements = fd.read()
        for statement in statements.split(";"):
            print(statement)
            con.execute(statement)

def load_json_file(name: str) -> None:
    data = {}
    with open(f"data/raw_data/{today_date}/{name}") as fd:
        data = json.load(fd)
    return data

def paris_consolidate_station_data() -> None:
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)

    data = load_json_file("paris_realtime_bicycle_data.json")
    paris_raw_data_df = pd.json_normalize(data)
    paris_raw_data_df["id"] = paris_raw_data_df["stationcode"].apply(lambda x: f"{PARIS_CITY_CODE}-{x}")
    paris_raw_data_df["address"] = None
    paris_raw_data_df["created_date"] = date.today()

    paris_station_data_df = paris_raw_data_df[[
        "id",
        "stationcode",
        "name",
        "nom_arrondissement_communes",
        "code_insee_commune",
        "address",
        "coordonnees_geo.lon",
        "coordonnees_geo.lat",
        "is_installed",
        "created_date",
        "capacity"
    ]]

    paris_station_data_df.rename(columns={
        "stationcode": "code",
        "name": "name",
        "coordonnees_geo.lon": "longitude",
        "coordonnees_geo.lat": "latitude",
        "is_installed": "status",
        "nom_arrondissement_communes": "city_name",
        "code_insee_commune": "city_code"
    }, inplace=True)

    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM paris_station_data_df;")

def get_city_code(name: str)  -> None:
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)

    requete = f"""SELECT ID FROM CONSOLIDATE_CITY 
             WHERE lower(NAME) = '{name}'
             AND CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_CITY)"""
    code = con.sql(requete).fetchall()[0][0]
    return code

def nantes_consolidate_station_data() -> None:

    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)

    data = load_json_file("nantes_realtime_bicycle_data.json")
    nantes_raw_data_df = pd.json_normalize(data)
    nantes_raw_data_df["id"] = nantes_raw_data_df["number"].apply(lambda x: f"{NANTES_CITY_CODE}-{x}")
    nantes_raw_data_df["city_code"] = get_city_code("nantes")
    nantes_raw_data_df["created_date"] = date.today()

    nantes_station_data_df = nantes_raw_data_df[[
        "id",
        "number",
        "name",
        "contract_name",
        "city_code",
        "address",
        "position.lon",
        "position.lat",
        "status",
        "created_date",
        "bike_stands"
    ]]

    nantes_station_data_df.rename(columns={
        "number": "code",
        "contract_name" : "city_name",
        "position.lon" : "longitude",
        "position.lat": "latitude",
        "bike_stands": "capacity",

    }, inplace=True)

    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM nantes_station_data_df;")

def toulouse_consolidate_station_data() -> None:

    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)

    data = load_json_file("toulouse_realtime_bicycle_data.json")
    toulouse_raw_data_df = pd.json_normalize(data)
    toulouse_raw_data_df["id"] = toulouse_raw_data_df["number"].apply(lambda x: f"{TOULOUSE_CITY_CODE}-{x}")
    toulouse_raw_data_df["city_code"] = get_city_code("toulouse")
    toulouse_raw_data_df["created_date"] = date.today()

    toulouse_station_data_df = toulouse_raw_data_df[[
        "id",
        "number",
        "name",
        "contract_name",
        "city_code",
        "address",
        "position.lon",
        "position.lat",
        "status",
        "created_date",
        "bike_stands"
    ]]

    toulouse_station_data_df.rename(columns={
        "number": "code",
        "contract_name" : "city_name",
        "position.lon" : "longitude",
        "position.lat": "latitude",
        "bike_stands": "capacity",

    }, inplace=True)

    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM toulouse_station_data_df;")

def consolidate_station_data() -> None:

    paris_consolidate_station_data()

    nantes_consolidate_station_data()

    toulouse_consolidate_station_data()

def consolidate_city_data() -> None:

    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)

    data = load_json_file("commune_data.json")
    raw_data_df = pd.json_normalize(data)
    commune_df = raw_data_df[["code","nom","population"]]

    commune_df.rename(columns={
        "code": "id",
        "nom": "name",
        "population" : "nb_inhabitants"
    }, inplace=True)
    commune_df.drop_duplicates(inplace = True)

    commune_df["created_date"] = date.today()
    
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_CITY SELECT * FROM commune_df;")

def paris_consolidate_station_statement_data() -> None:

    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)

    data = load_json_file("paris_realtime_bicycle_data.json")
    paris_raw_data_df = pd.json_normalize(data)
    paris_raw_data_df["station_id"] = paris_raw_data_df["stationcode"].apply(lambda x: f"{PARIS_CITY_CODE}-{x}")
    paris_raw_data_df["created_date"] = date.today()
    paris_station_statement_data_df = paris_raw_data_df[[
        "station_id",
        "numdocksavailable",
        "numbikesavailable",
        "duedate",
        "created_date"
    ]]
    
    paris_station_statement_data_df.rename(columns={
        "numdocksavailable": "bicycle_docks_available",
        "numbikesavailable": "bicycle_available",
        "duedate": "last_statement_date",
    }, inplace=True)

    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM paris_station_statement_data_df;")

def nantes_consolidate_station_statement_data() -> None:
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)

    data = load_json_file("nantes_realtime_bicycle_data.json")
    nantes_raw_data_df = pd.json_normalize(data)
    nantes_raw_data_df["station_id"] = nantes_raw_data_df["number"].apply(lambda x: f"{NANTES_CITY_CODE}-{x}")
    nantes_raw_data_df["created_date"] = date.today()
    nantes_station_statement_data_df = nantes_raw_data_df[[
        "station_id",
        "available_bike_stands",
        "available_bikes",
        "last_update",
        "created_date"
    ]]
    
    nantes_station_statement_data_df.rename(columns={
        "available_bike_stands": "bicycle_docks_available",
        "available_bikes": "bicycle_available",
        "last_update": "last_statement_date",
    }, inplace=True)

    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM nantes_station_statement_data_df;")

def toulouse_consolidate_station_statement_data() -> None:
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)

    data = load_json_file("toulouse_realtime_bicycle_data.json")
    toulouse_raw_data_df = pd.json_normalize(data)
    toulouse_raw_data_df["station_id"] = toulouse_raw_data_df["number"].apply(lambda x: f"{TOULOUSE_CITY_CODE}-{x}")
    toulouse_raw_data_df["created_date"] = date.today()
    toulouse_station_statement_data_df = toulouse_raw_data_df[[
        "station_id",
        "available_bike_stands",
        "available_bikes",
        "last_update",
        "created_date"
    ]]
    
    toulouse_station_statement_data_df.rename(columns={
        "available_bike_stands": "bicycle_docks_available",
        "available_bikes": "bicycle_available",
        "last_update": "last_statement_date",
    }, inplace=True)

    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM toulouse_station_statement_data_df;")

def consolidate_station_statement_data() -> None:

    paris_consolidate_station_statement_data()
    
    nantes_consolidate_station_statement_data()

    toulouse_consolidate_station_statement_data()