import json
from datetime import datetime, date
import duckdb
import pandas as pd
import logging


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

duckdb_path = "data/duckdb/mobility_analysis.duckdb"  # Chemin d'accès du fichier mobility_analysis.duckdb


def create_consolidate_tables() -> None:
    """
    Crée les tables définies dans un fichier SQL.

    Les instructions SQL sont lues depuis un fichier `create_agregate_tables.sql`,
    situé dans le répertoire `data/sql_statements`, et exécutées une par une
    sur la base de données `mobility_analysis.duckdb`.
    """

    con = duckdb.connect(database=duckdb_path, read_only=False)
    with open("data/sql_statements/create_consolidate_tables.sql") as fd:
        statements = fd.read()

        # Exécution de chaque instruction SQL séparée par un ";"
        for statement in statements.split(";"):
            con.execute(statement)
        logger.info("Consolidate tables created if they didn't exist.")


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
    3. Insère ou remplace ces données dans la table `CONSOLIDATE_STATION` de la base mobility_analysis.
    """
    con = duckdb.connect(database=duckdb_path, read_only=False)

    con.execute(
    f"""insert or replace into consolidate_station 
        select 
              {PARIS_CITY_CODE} || '-' || stationcode as id,
              stationcode AS code,
              name,
              nom_arrondissement_communes as city_name,
              code_insee_commune as city_code,
              Null as address,
              coordonnees_geo.lon as longitude,
              coordonnees_geo.lat as latitude,
              is_installed as status,
              now() as created_date,
              capacity
              from read_json('data/raw_data/{today_date}/paris_realtime_bicycle_data.json')"""
    )


def get_city_code(name: str) -> str:
    """
    Récupère le code d'une ville depuis la table `CONSOLIDATE_CITY` en fonction de son nom.

    Args:
        name (str): Nom de la ville (non sensible à la casse).

    Returns:
        str: Code de la ville correspondante.

    Note:
        - Le code est extrait des données les plus récentes (basées sur la colonne `CREATED_DATE`).
    """

    con = duckdb.connect(database=duckdb_path, read_only=False)

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
    3. Ajoute ou met à jour les données consolidées des stations dans la table `CONSOLIDATE_STATION` de la base mobility_analysis
    """

    # Liste des villes et de leurs codes correspondants
    cities = ["nantes", "toulouse"]
    cities_code = [NANTES_CITY_CODE, TOULOUSE_CITY_CODE]

    for city, city_code in zip(cities, cities_code):
        con = duckdb.connect(database=duckdb_path, read_only=False)

        insse_code = get_city_code(city)  # Récupération du code INSEE de la ville
    
        con.execute(f"""
        insert or replace into consolidate_station 
        select
                {city_code} || '-' || number as id,
                number as code,
                name,
                contract_name as city_name,
                {insse_code} as city_code,
                address,
                position.lon as longitude,
                position.lat as latitude,
                status,
                now() as created_date,
                bike_stands as capacity,
        from read_json('data/raw_data/{today_date}/{city}_realtime_bicycle_data.json')
        """)

        con.close()


def strasbourg_consolidate_station_data() -> None:
    """
    Consolidation des données des stations de vélos à Strasbourg.

    Cette fonction :
    1. Charge les données brutes des stations de vélos à Strasbourg depuis un fichier JSON.
    2. Transforme et nettoie les données pour les aligner avec le format attendu.
    3. Insère ou remplace ces données dans la table `CONSOLIDATE_STATION` de la base mobility_analysis.
    """

    con = duckdb.connect(database=duckdb_path, read_only=False)
    # Le nom de la ville n'existe pas dans les données brute. Il faut donc l'encodé en dure dans le code

    insse_code = get_city_code("strasbourg")

    con.execute(f"""
    insert or replace into consolidate_station
    select
        {STRASBOURG_CITY_CODE} || '-' || id as id,
        id as code,
        na as name,
        'strasbourg' as city_name,
        {insse_code} as city_code,
        null as address,
        lon as longitude,
        lat as latitude,
        is_installed as status,
        now() as created_date,
        "to" as capacity
    from read_json('data/raw_data/{today_date}/strasbourg_realtime_bicycle_data.json')
    """)


def consolidate_station_data() -> None:

    paris_consolidate_station_data()

    nantes_toulouse_consolidate_station_data()

    strasbourg_consolidate_station_data()


def consolidate_city_data() -> None:
    """
    Consolidation des données des communes.

    Cette fonction :
    1. Charge les données brutes des communes depuis un fichier JSON.
    2. Transforme et nettoie les données pour les aligner avec le format attendu.
    3. Insère ou remplace ces données dans la table `CONSOLIDATE_CITY` de la base mobility_analysis.

    Les données incluent des informations telles que l'identifiant INSEE, le nom de la commune
    et sa population.
    """

    con = duckdb.connect(database=duckdb_path, read_only=False)

    data = load_json_file("commune_data.json")
    raw_data_df = pd.json_normalize(data)
    commune_df = raw_data_df.loc[:, ["code", "nom", "population"]]

    commune_df.rename(
        columns={"code": "id", "nom": "name", "population": "nb_inhabitants"},
        inplace=True,
    )

    commune_df["created_date"] = date.today()

    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_CITY SELECT * FROM commune_df;")


def paris_consolidate_station_statement_data() -> None:
    """
    Consolidation des données de disponibilité des stations de vélos à Paris.

    Cette fonction :
    1. Charge les données brutes des disponibilités des stations de vélos à Paris depuis un fichier JSON.
    2. Transforme et nettoie les données pour les aligner avec le format attendu.
    3. Insère ou remplace ces données dans la table `CONSOLIDATE_STATION_STATEMENT` de la base mobility_analysis.
    """

    con = duckdb.connect(database=duckdb_path, read_only=False)

    data = load_json_file("paris_realtime_bicycle_data.json")
    paris_raw_data_df = pd.json_normalize(data)
    paris_raw_data_df["station_id"] = paris_raw_data_df["stationcode"].apply(
        lambda x: f"{PARIS_CITY_CODE}-{x}"
    )
    paris_raw_data_df["created_date"] = date.today()
    paris_station_statement_data_df = paris_raw_data_df.loc[
        :,
        [
            "station_id",
            "numdocksavailable",
            "numbikesavailable",
            "duedate",
            "created_date",
        ],
    ]

    paris_station_statement_data_df.rename(
        columns={
            "numdocksavailable": "bicycle_docks_available",
            "numbikesavailable": "bicycle_available",
            "duedate": "last_statement_date",
        },
        inplace=True,
    )

    con.execute(
        "INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM paris_station_statement_data_df;"
    )


def nantes_toulouse_consolidate_station_statement_data() -> None:
    """
    Consolide les données de l'état des stations de vélos en libre-service pour Nantes et Toulouse.

    Cette fonction :
    1. Charge les données brutes pour chaque ville depuis des fichiers JSON.
    2. Transforme et nettoie les données pour les aligner avec le format attendu.
    3. Ajoute ou met à jour les données consolidées des états de stations dans la table
       `CONSOLIDATE_STATION_STATEMENT` de la base mobility_analysis.
    """

    # Liste des villes et de leurs codes correspondants
    cities = ["nantes", "toulouse"]
    cities_code = [NANTES_CITY_CODE, TOULOUSE_CITY_CODE]

    for city, city_code in zip(cities, cities_code):
        con = duckdb.connect(database=duckdb_path, read_only=False)

        data = load_json_file(f"{city}_realtime_bicycle_data.json")
        raw_data_df = pd.json_normalize(data)
        raw_data_df["station_id"] = raw_data_df["number"].apply(
            lambda x: f"{city_code}-{x}"
        )
        raw_data_df["created_date"] = date.today()
        station_statement_data_df = raw_data_df.loc[
            :,
            [
                "station_id",
                "available_bike_stands",
                "available_bikes",
                "last_update",
                "created_date",
            ],
        ]

        station_statement_data_df.rename(
            columns={
                "available_bike_stands": "bicycle_docks_available",
                "available_bikes": "bicycle_available",
                "last_update": "last_statement_date",
            },
            inplace=True,
        )

        con.execute(
            "INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM station_statement_data_df;"
        )
        con.close()


def strasbourg_consolidate_station_statement_data() -> None:
    """
    Consolidation des données de disponibilité des stations de vélos à Strasbourg.

    Cette fonction :
    1. Charge les données brutes des disponibilités des stations de vélos à Strasbourg depuis un fichier JSON.
    2. Transforme et nettoie les données pour les aligner avec le format attendu.
    3. Insère ou remplace ces données dans la table `CONSOLIDATE_STATION_STATEMENT` de la base mobility_analysis.
    """

    con = duckdb.connect(database=duckdb_path, read_only=False)

    data = load_json_file("strasbourg_realtime_bicycle_data.json")
    strasbourg_raw_data_df = pd.json_normalize(data)
    strasbourg_raw_data_df["station_id"] = strasbourg_raw_data_df["id"].apply(
        lambda x: f"{STRASBOURG_CITY_CODE}-{x}"
    )
    # Convertion du format timestamp Unix en datetime
    strasbourg_raw_data_df["last_statement_date"] = strasbourg_raw_data_df[
        "last_reported"
    ].apply(lambda x: datetime.fromtimestamp(int(x)).strftime("%Y-%m-%d"))
    strasbourg_raw_data_df["created_date"] = date.today()

    strasbourg_station_statement_data_df = strasbourg_raw_data_df.loc[
        :,
        [
            "station_id",
            "num_docks_available",
            "av",
            "last_statement_date",
            "created_date",
        ],
    ]

    strasbourg_station_statement_data_df.rename(
        columns={
            "num_docks_available": "bicycle_docks_available",
            "av": "bicycle_available",
        },
        inplace=True,
    )

    con.execute(
        "INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM strasbourg_station_statement_data_df;"
    )


def consolidate_station_statement_data() -> None:

    paris_consolidate_station_statement_data()

    nantes_toulouse_consolidate_station_statement_data()

    strasbourg_consolidate_station_statement_data()
