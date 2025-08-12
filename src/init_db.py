import logging

from duckdb_tools import exec_sql_from_file

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


if __name__ == "__main__":
    logger.info("Initialisation de la base de données.")

    table_definitions = {
        "create_consolidate_tables.sql": "Création des tables consolidées si elles n'existent pas.",
        "create_agregate_tables.sql": "Création des tables d'agrégation si elles n'existent pas."
    }

    for file_name, log_message in table_definitions.items():
        exec_sql_from_file(file_name, log_message)

    logger.info("Base de données initialisée.")
