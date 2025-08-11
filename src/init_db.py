import logging

import duckdb

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

duckdb_path = "data/duckdb/mobility_analysis.duckdb"


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


def init_db() -> None:
    """
    Initialise la base de données en créant les tables nécessaires.

    Cette fonction appelle les fonctions pour créer les tables de consolidation
    et d'agrégation.
    """
    logger.info("Initialisation de la base de données.")
    create_consolidate_tables()
    create_agregate_tables()
    logger.info("Base de données initialisée.")
