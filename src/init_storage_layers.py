"""
Script d’initialisation de la base DuckDB pour l’analyse de mobilité.

Fonctionnalités principales :
- Lit et exécute les fichiers SQL situés dans `src/sql_statements/`.
- Crée les tables consolidées et d’agrégation si elles n’existent pas.
- Journalise les étapes d’initialisation.
"""

import logging

from utils import exec_sql_from_file, init_minio_bucket

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger: logging.Logger = logging.getLogger(__name__)


if __name__ == "__main__":

    logger.info("Initialisation de la base de données DuckDB.")

    table_definitions: dict[str, str] = {
        "create_consolidate_tables.sql": "Création des tables consolidées si elles n'existent pas.",
        "create_agregate_tables.sql": "Création des tables d'agrégation si elles n'existent pas.",
    }

    for file_name, log_message in table_definitions.items():
        exec_sql_from_file(file_name, log_message)
    logger.info("Initialisation de MinIO.")
    init_minio_bucket()
