import json
from datetime import datetime, date

import duckdb
import pandas as pd

today_date = datetime.now().strftime("%Y-%m-%d")
PARIS_CITY_CODE = 1
NANTES_CITY_CODE = 2
TOULOUSE_CITY_CODE = 3
duckdb_path = "data/duckdb/mobility_analysis.duckdb"

def create_consolidate_tables() -> None:
    """
    Crée les tables définies dans un fichier SQL.

    Les instructions SQL sont lues depuis un fichier `create_agregate_tables.sql`,
    situé dans le répertoire `data/sql_statements`, et exécutées une par une
    sur la base de données `mobility_analysis.duckdb`.
    """

    con = duckdb.connect(database = duckdb_path, read_only = False)
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
    con = duckdb.connect(database = duckdb_path, read_only = False)

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

    con = duckdb.connect(database = duckdb_path, read_only = False)

    requete = f"""SELECT ID FROM CONSOLIDATE_CITY 
             WHERE lower(NAME) = '{name}'
             AND CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_CITY)"""
    code = con.sql(requete).fetchall()[0][0]

    return code

def nantes_toulouse_consolidate_station_data() -> None:
    """
    Consolide les données des stations de vélos en libre-service pour Nantes et Toulouse.

    Cette fonction :
    1. Charge les données brutes pour chaque ville depuis des fichiers JSON.
    2. Transforme et nettoie les données pour les aligner avec le format attendu.
    3. Ajoute ou met à jour les données consolidées des stations dans la table `CONSOLIDATE_STATION` de la base DuckDB.

    Les données incluent des informations sur les stations, telles que :
    - Identifiant unique (basé sur le code de la ville et le numéro de station),
    - Nom de la station, coordonnées, adresse, capacité, et statut.
    """

    # Liste des villes et de leurs codes correspondants
    cities = ["nantes", "toulouse"]
    cities_code = [NANTES_CITY_CODE, TOULOUSE_CITY_CODE]

    for city, city_code in zip(cities, cities_code):
        con = duckdb.connect(database = duckdb_path, read_only = False)

        data = load_json_file(f"{city}_realtime_bicycle_data.json")
        raw_data_df = pd.json_normalize(data)
        raw_data_df["id"] = raw_data_df["number"].apply(lambda x: f"{city_code}-{x}")
        raw_data_df["city_code"] = get_city_code(city) # Récupération du code INSEE de la ville
        raw_data_df["created_date"] = date.today()

        station_data_df = raw_data_df[[
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

        station_data_df.rename(columns={
            "number": "code",
            "contract_name" : "city_name",
            "position.lon" : "longitude",
            "position.lat": "latitude",
            "bike_stands": "capacity",

        }, inplace=True)

        con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM station_data_df;")
        con.close()

def consolidate_station_data() -> None:

    paris_consolidate_station_data()

    nantes_toulouse_consolidate_station_data()

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

    con = duckdb.connect(database = duckdb_path, read_only = False)

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

    con = duckdb.connect(database = duckdb_path, read_only = False)

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

def nantes_toulouse_consolidate_station_statement_data() -> None:
    """
    Consolide les données de l'état des stations de vélos en libre-service pour Nantes et Toulouse.

    Cette fonction :
    1. Charge les données brutes pour chaque ville depuis des fichiers JSON.
    2. Transforme et nettoie les données pour les aligner avec le format attendu.
    3. Ajoute ou met à jour les données consolidées des états de stations dans la table
       `CONSOLIDATE_STATION_STATEMENT` de la base DuckDB.

    Les données incluent des informations sur :
    - Le nombre de vélos disponibles (`bicycle_available`),
    - Le nombre de bornes disponibles (`bicycle_docks_available`),
    - La date du dernier relevé (`last_statement_date`).
    """
    
    # Liste des villes et de leurs codes correspondants
    cities = ["nantes", "toulouse"]
    cities_code = [NANTES_CITY_CODE, TOULOUSE_CITY_CODE]

    for city, city_code in zip(cities, cities_code):    
        con = duckdb.connect(database = duckdb_path, read_only = False)

        data = load_json_file(f"{city}_realtime_bicycle_data.json")
        raw_data_df = pd.json_normalize(data)
        raw_data_df["station_id"] = raw_data_df["number"].apply(lambda x: f"{city_code}-{x}")
        raw_data_df["created_date"] = date.today()
        station_statement_data_df = raw_data_df[[
            "station_id",
            "available_bike_stands",
            "available_bikes",
            "last_update",
            "created_date"
        ]]
        
        station_statement_data_df.rename(columns={
            "available_bike_stands": "bicycle_docks_available",
            "available_bikes": "bicycle_available",
            "last_update": "last_statement_date",
        }, inplace=True)

        con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM station_statement_data_df;")
        con.close()

def consolidate_station_statement_data() -> None:

    paris_consolidate_station_statement_data()

    nantes_toulouse_consolidate_station_statement_data()