import json
from datetime import datetime, date

import duckdb
import pandas as pd

today_date = datetime.now().strftime("%Y-%m-%d")
PARIS_CITY_CODE = 1
NANTES_CITY_CODE = 2
TOULOUSE_CITY_CODE = 3

def create_consolidate_tables() -> None:
    """
    Crée les tables définies dans un fichier SQL.

    Les instructions SQL sont lues depuis un fichier `create_agregate_tables.sql`,
    situé dans le répertoire `data/sql_statements`, et exécutées une par une
    sur la base de données `mobility_analysis.duckdb`.
    """

    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    with open("data/sql_statements/create_consolidate_tables.sql") as fd:
        statements = fd.read()

        # Exécution de chaque instruction SQL séparée par un ";"
        for statement in statements.split(";"):
            print(statement)
            con.execute(statement)

def load_json_file(name: str) -> list[dict]:
    """
    Charge un fichier JSON depuis le répertoire des données brutes pour la date du jour.

    Args:
        name (str): Nom du fichier JSON (incluant l'extension).

    Returns:
        list[dict]: Données chargées depuis le fichier JSON sous forme de liste de dictionnaires.
    """
    data = {}

    with open(f"data/raw_data/{today_date}/{name}") as fd:
        data = json.load(fd)

    return data

def paris_consolidate_station_data() -> None:
    """
    Consolidation des données des stations de vélos à Paris.

    Cette fonction :
    1. Charge les données brutes des stations de vélos à Paris depuis un fichier JSON.
    2. Transforme et nettoie les données pour les aligner avec le format attendu.
    3. Insère ou remplace ces données dans la table `CONSOLIDATE_STATION` de la base DuckDB.

    Les données incluent des informations telles que le code de la station, son nom,
    sa localisation géographique, sa capacité, et son statut.
    """
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

def get_city_code(name: str)  -> str:
    """
    Récupère le code d'une ville depuis la table `CONSOLIDATE_CITY` en fonction de son nom.

    Args:
        name (str): Nom de la ville (non sensible à la casse).

    Returns:
        str: Code de la ville correspondante.

    Note:
        - La requête sélectionne la ville dont le nom correspond (indépendamment de la casse).
        - Le code est extrait des données les plus récentes (basées sur la colonne `CREATED_DATE`).
    """

    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)

    requete = f"""SELECT ID FROM CONSOLIDATE_CITY 
             WHERE lower(NAME) = '{name}'
             AND CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_CITY)"""
    code = con.sql(requete).fetchall()[0][0]

    return code

def nantes_consolidate_station_data() -> None:
    """
    Consolidation des données des stations de vélos à Paris.

    Cette fonction :
    1. Charge les données brutes des stations de vélos à Paris depuis un fichier JSON.
    2. Transforme et nettoie les données pour les aligner avec le format attendu.
    3. Insère ou remplace ces données dans la table `CONSOLIDATE_STATION` de la base DuckDB.

    Les données incluent des informations telles que le code de la station, son nom,
    sa localisation géographique, sa capacité, et son statut.
    """

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
    """
    Consolidation des données des stations de vélos à Paris.

    Cette fonction :
    1. Charge les données brutes des stations de vélos à Paris depuis un fichier JSON.
    2. Transforme et nettoie les données pour les aligner avec le format attendu.
    3. Insère ou remplace ces données dans la table `CONSOLIDATE_STATION` de la base DuckDB.

    Les données incluent des informations telles que le code de la station, son nom,
    sa localisation géographique, sa capacité, et son statut.
    """

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
    """
    Consolidation des données des communes.

    Cette fonction :
    1. Charge les données brutes des communes depuis un fichier JSON.
    2. Transforme et nettoie les données pour les aligner avec le format attendu.
    3. Supprime les doublons.
    4. Insère ou remplace ces données dans la table `CONSOLIDATE_CITY` de la base DuckDB.

    Les données incluent des informations telles que l'identifiant INSEE, le nom de la commune
    et sa population.
    """

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
    """
    Consolidation des données de disponibilité des stations de vélos à Paris.

    Cette fonction :
    1. Charge les données brutes des disponibilités des stations de vélos à Paris depuis un fichier JSON.
    2. Transforme et nettoie les données pour les aligner avec le format attendu.
    3. Insère ou remplace ces données dans la table `CONSOLIDATE_STATION_STATEMENT` de la base DuckDB.

    Les données incluent des informations sur les stations, telles que le nombre de vélos
    disponibles, les bornes disponibles, et la dernière date d'actualisation.
    """

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
    """
    Consolidation des données de disponibilité des stations de vélos à Paris.

    Cette fonction :
    1. Charge les données brutes des disponibilités des stations de vélos à Paris depuis un fichier JSON.
    2. Transforme et nettoie les données pour les aligner avec le format attendu.
    3. Insère ou remplace ces données dans la table `CONSOLIDATE_STATION_STATEMENT` de la base DuckDB.

    Les données incluent des informations sur les stations, telles que le nombre de vélos
    disponibles, les bornes disponibles, et la dernière date d'actualisation.
    """

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
    """
    Consolidation des données de disponibilité des stations de vélos à Paris.

    Cette fonction :
    1. Charge les données brutes des disponibilités des stations de vélos à Paris depuis un fichier JSON.
    2. Transforme et nettoie les données pour les aligner avec le format attendu.
    3. Insère ou remplace ces données dans la table `CONSOLIDATE_STATION_STATEMENT` de la base DuckDB.

    Les données incluent des informations sur les stations, telles que le nombre de vélos
    disponibles, les bornes disponibles, et la dernière date d'actualisation.
    """
    
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