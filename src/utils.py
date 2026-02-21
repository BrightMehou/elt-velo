"""
Module d'utilitaires pour DuckDB et MinIO.

Fonctions réutilisables pour :
- Exécution de fichiers SQL sur DuckDB
- Initialisation de buckets MinIO
- Envoi de fichiers JSON vers MinIO
- Exécute les transformations ELT via `dbt run` dans le projet `src/elt`.
"""

import logging
import os
import subprocess
from datetime import datetime

import psycopg2

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger: logging.Logger = logging.getLogger(__name__)


today_date: str = datetime.now().strftime("%Y-%m-%d")

DB_NAME: str = os.getenv("DB_NAME", "postgres")
DB_USER: str = os.getenv("DB_USER", "postgres")
DB_PASSWORD: str = os.getenv("DB_PASSWORD", "postgres") 
DB_HOST: str = os.getenv("DB_HOST", "localhost")
DB_PORT: str = os.getenv("DB_PORT", "5432")

def exec_sql_from_file(
    file_name: str,
    log_message: str,
) -> None:
    """
    Exécute les instructions SQL d'un fichier sur une base de données PostgreSQL.

    Args:
        file_name (str): Le nom du fichier SQL situé dans `src/sql_statements`.
        log_message (str): Message à afficher dans les logs après l'exécution.
    """
    sql_path: str = f"src/sql_statements/{file_name}"
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

    with open(sql_path) as fd:
        query: str = fd.read()

        with conn.cursor() as cursor:
            cursor.execute(query)
            conn.commit()
            logger.info(log_message)
  

def store_json(name: str, raw_json: str) -> None:
    """
    Envoie les données JSON dans la table staging_raw de PostgreSQL.

    Args:
        name (str): Le nom du fichier source
        raw_json (str): Les données brutes en format JSON
    """
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

    with conn.cursor() as cursor:
        insert_query = f"INSERT INTO staging_raw (nom, date, data) VALUES (%s, %s, %s) ON CONFLICT (nom, date) DO UPDATE SET data = EXCLUDED.data"
        cursor.execute(insert_query, (name, today_date, raw_json))
        conn.commit()
        logger.info(f"Données JSON insérées dans la table staging_raw de PostgreSQL.")
      
def data_transformation() -> None:
    """
    Exécute la commande `dbt run` et affiche les logs en temps réel
    directement dans le terminal (sans capture ni buffering).
    """

    logger.info("🚀 Démarrage de la commande dbt run")

    try:
        subprocess.run(
            [
                "dbt",
                "run",
                "--project-dir",
                "src/transformation",
                "--profiles-dir",
                "src/transformation",
            ],
            check=True,
        )
        logger.info("✅ dbt run terminé avec succès")

    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Erreur pendant le dbt run (code {e.returncode})")
