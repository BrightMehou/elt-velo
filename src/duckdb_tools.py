import logging
import os

import duckdb

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

duckdb_path = "data/duckdb/mobility_analysis.duckdb"

def exec_sql_from_file(
    file_name: str, log_message: str,
) -> None:
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

