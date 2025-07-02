from datetime import datetime
import duckdb
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
    f"""
        create temp table Paris AS select * from read_json('data/raw_data/{today_date}/paris_realtime_bicycle_data.json');

        insert or replace into consolidate_station 
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
              current_date() as created_date,
              capacity
              from paris;

        insert or replace into CONSOLIDATE_STATION_STATEMENT
        select 
                {PARIS_CITY_CODE} || '-' || stationcode as station_id,
                numdocksavailable AS bicycle_docks_available,
                numbikesavailable AS bicycle_available,
                duedate as last_statement_date,
                current_date() as created_date,
        from Paris;
    """
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
    
        con.execute(
        f"""
        create temp table {city} AS select * from read_json('data/raw_data/{today_date}/{city}_realtime_bicycle_data.json');
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
                current_date() as created_date,
                bike_stands as capacity,
        from {city};

        INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT
        select
            {city_code} || '-' || number as station_id,
            available_bike_stands as bicycle_docks_available,
            available_bikes as bicycle_available,
            last_update as last_statement_date,
            current_date() as created_date
        from {city};
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

    con.execute(
    f"""
    create temp table Strasbourg AS select * from read_json('data/raw_data/{today_date}/strasbourg_realtime_bicycle_data.json');
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
        current_date() as created_date,
        "to" as capacity
    from Strasbourg;

    INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT 
    select
        {STRASBOURG_CITY_CODE} || '-' || id as station_id,
        num_docks_available as bicycle_docks_available,
        av as bicycle_available,
        to_timestamp(last_reported::int) as last_statement_date,
        current_date() as created_date
    from Strasbourg;
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


    con.execute(f"""INSERT OR REPLACE into consolidate_city
                    select 
                        code as id,
                        nom as name,
                        population as nb_inhabitants,
                    now() as created_date
                    from read_json('data/raw_data/{today_date}/commune_data.json')""") 

