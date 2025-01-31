import duckdb
import logging


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
duckdb_path = "data/duckdb/mobility_analysis.duckdb"


def create_agregate_tables() -> None:
    """
    Crée les tables définies dans un fichier SQL.

    Les instructions SQL sont lues depuis un fichier `create_agregate_tables.sql`,
    situé dans le répertoire `data/sql_statements`, et exécutées une par une
    sur la base de données `mobility_analysis.duckdb`.
    """

    con = duckdb.connect(database=duckdb_path, read_only=False)
    with open("data/sql_statements/create_agregate_tables.sql") as fd:
        statements = fd.read()

        # Exécution de chaque instruction SQL séparée par un ";"
        for statement in statements.split(";"):
            con.execute(statement)
        logger.info("Agregate tables created if they didn't exist.")


def agregate_dim_station() -> None:
    """
    Insère ou remplace les données dans la table DIM_STATION avec les informations
    les plus récentes depuis la table CONSOLIDATE_STATION.

    Les données insérées contiennent des informations sur les stations de vélos.
    """

    con = duckdb.connect(database=duckdb_path, read_only=False)

    sql_statement = """
    INSERT OR REPLACE INTO DIM_STATION
    SELECT 
        ID,
        CODE,
        NAME,
        ADDRESS,
        LONGITUDE,
        LATITUDE,
        STATUS,
        CAPACITTY
    FROM CONSOLIDATE_STATION
    WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION);
    """

    con.execute(sql_statement)


def agregate_dim_city() -> None:
    """
    Insère ou remplace les données dans la table DIM_CITY avec les informations
    les plus récentes depuis la table CONSOLIDATE_CITY.

    Les données insérées contiennent des informations sur les villes, comme le
    nombre d'habitants.
    """

    con = duckdb.connect(database=duckdb_path, read_only=False)

    sql_statement = """
    INSERT OR REPLACE INTO DIM_CITY
    SELECT 
        ID,
        NAME,
        NB_INHABITANTS
    FROM CONSOLIDATE_CITY
    WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_CITY);
    """

    con.execute(sql_statement)


def agregate_fact_station_statements() -> None:
    """
    Insère ou remplace les données dans la table FACT_STATION_STATEMENT avec les informations
    les plus récentes depuis la table CONSOLIDATE_STATION_STATEMENT.

    Les données insérées contiennent les déclarations d'état des stations de vélos,
    incluant les vélos disponibles, les bornes libres, et des informations liées aux
    villes correspondantes.
    """

    con = duckdb.connect(database=duckdb_path, read_only=False)

    # First we agregate the Paris station statement data
    sql_statement = """
    INSERT OR REPLACE INTO FACT_STATION_STATEMENT
    SELECT STATION_ID, cc.ID as CITY_ID, BICYCLE_DOCKS_AVAILABLE, BICYCLE_AVAILABLE, LAST_STATEMENT_DATE, current_date as CREATED_DATE
    FROM CONSOLIDATE_STATION_STATEMENT
    JOIN CONSOLIDATE_STATION ON CONSOLIDATE_STATION.ID = CONSOLIDATE_STATION_STATEMENT.STATION_ID
    LEFT JOIN CONSOLIDATE_CITY as cc ON cc.ID = CONSOLIDATE_STATION.CITY_CODE
    WHERE CITY_CODE != 0 
        AND CONSOLIDATE_STATION_STATEMENT.CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION_STATEMENT)
        AND CONSOLIDATE_STATION.CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION)
        AND cc.CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_CITY);
    """

    con.execute(sql_statement)
