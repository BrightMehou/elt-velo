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
from io import BytesIO

import duckdb
from minio import Minio

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger: logging.Logger = logging.getLogger(__name__)

DUCKDB_PATH: str = "data/duckdb/mobility_analysis.duckdb"
BUCKET_NAME: str = "mobility-analysis"

MINIO_ENDPOINT: str = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY: str = os.getenv("MINIO_SECRET_KEY", "miniopassword")

today_date: str = datetime.now().strftime("%Y-%m-%d")


def exec_sql_from_file(
    file_name: str,
    log_message: str,
) -> None:
    """
    Crée les tables définies dans un fichier SQL.

    Les instructions SQL sont lues depuis le fichier spécifié,
    situé dans le répertoire `src/sql_statements`, et exécutées
    une par une sur la base de données `mobility_analysis.duckdb`.
    """
    con = duckdb.connect(database=DUCKDB_PATH, read_only=False)
    sql_path: str = f"src/sql_statements/{file_name}"

    with open(sql_path) as fd:
        statements: str = fd.read()

        for statement in statements.split(";"):
            con.execute(statement)
        logger.info(log_message)


minio_client: Minio = Minio(
    endpoint=MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False,
)


def init_minio_bucket() -> None:
    """Crée un bucket dans MinIO s'il n'existe pas."""
    if not minio_client.bucket_exists(BUCKET_NAME):
        minio_client.make_bucket(BUCKET_NAME)
        logger.info(f"Bucket '{BUCKET_NAME}' créé.")


def store_json(raw_json: str, file_name: str) -> None:
    """
    Envoie les données JSON dans un bucket MinIO.
    Structure du chemin : YYYY-MM-DD/nom_fichier.json

    Args:
        raw_json (str): Les données brutes en format JSON
        file_name (str): Fichier de sauvegarde des données
    """
    # Conversion en octets et convertion en "fichier virtuel en mémoire"
    object_key: str = f"{today_date}/{file_name}"
    data_bytes: bytes = raw_json.encode("utf-8")
    data_stream = BytesIO(data_bytes)

    minio_client.put_object(
        BUCKET_NAME,
        object_key,
        data_stream,
        length=len(data_bytes),
        content_type="application/json",
    )

    logger.info(f"Fichier envoyé dans MinIO : {BUCKET_NAME}/{object_key}")


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
