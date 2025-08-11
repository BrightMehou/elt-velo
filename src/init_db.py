import logging

import duckdb

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

duckdb_path = "data/duckdb/mobility_analysis.duckdb"

def create_tables_from_sql(file_name: str, log_message: str) -> None:
    """
    Crée les tables définies dans un fichier SQL.

    Les instructions SQL sont lues depuis le fichier spécifié,
    situé dans le répertoire `src/sql_statements`, et exécutées
    une par une sur la base de données `mobility_analysis.duckdb`.
    """
    con = duckdb.connect(database=duckdb_path, read_only=False)

    with open(f"src/sql_statements/{file_name}") as fd:
        statements = fd.read()

        for statement in statements.split(";"):
            con.execute(statement)
        logger.info(log_message)


if __name__ == "__main__":
    logger.info("Initialisation de la base de données.")

    table_definitions = {
        "create_consolidate_tables.sql": "Consolidate tables created if they didn't exist.",
        "create_agregate_tables.sql": "Agregate tables created if they didn't exist."
    }

    for file_name, log_message in table_definitions.items():
        create_tables_from_sql(file_name, log_message)

    logger.info("Base de données initialisée.")
