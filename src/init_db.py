"""
Script d'initialisation des couches de stockage pour l'analyse de mobilité.

Fonctionnalités principales :
- Création des tables de staging, consolidées et d'agrégation dans PostgreSQL.
"""

import logging

from utils import exec_sql_from_file

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", force=True
)
logger = logging.getLogger(__name__)


if __name__ == "__main__":
    logger.info("Initialisation de la base de données PostgreSQL.")

    TABLE_DEFINITIONS: dict[str, str] = {
        "create_staging_tables.sql": "Création des tables de staging",
#       "create_consolidate_tables.sql": "Création des tables consolidées",
#       "create_star_tables.sql": "Création des tables du modèle en étoile",
    }

    for file_name, log_message in TABLE_DEFINITIONS.items():
        exec_sql_from_file(file_name, log_message)