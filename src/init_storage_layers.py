"""
Script d'initialisation des couches de stockage pour l'analyse de mobilité.

Fonctionnalités principales :
- Création des tables consolidées et d'agrégation dans DuckDB.
- Initialisation du bucket MinIO pour le stockage des données.
"""

import logging

from utils import exec_sql_from_file, init_minio_bucket

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


if __name__ == "__main__":

    logger.info("Initialisation de la base de données DuckDB.")

    TABLE_DEFINITIONS: dict[str, str] = {
        "create_consolidate_tables.sql": "Création des tables consolidées si elles n'existent pas.",
        "create_agregate_tables.sql": "Création des tables d'agrégation si elles n'existent pas.",
    }

    for file_name, log_message in TABLE_DEFINITIONS.items():
        exec_sql_from_file(file_name, log_message)
    logger.info("Initialisation de MinIO.")

    init_minio_bucket()
