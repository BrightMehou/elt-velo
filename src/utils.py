"""
Module d'utilitaires pour DuckDB et MinIO.

Fonctions réutilisables pour :
- Exécution de fichiers SQL sur DuckDB
- Initialisation de buckets MinIO
- Envoi de fichiers JSON vers MinIO
"""

import logging
import os
from datetime import datetime
from io import BytesIO

import duckdb
from minio import Minio

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger: logging.Logger = logging.getLogger(__name__)

DUCKDB_PATH: str = "data/duckdb/mobility_analysis.duckdb"
BUCKET_NAME: str = "bicycle-data"

MINIO_ENDPOINT: str = os.getenv("minio_endpoint", "localhost:9000")
MINIO_ACCESS_KEY: str = os.getenv("minio_access_key", "minioadmin")
MINIO_SECRET_KEY: str = os.getenv("minio_secret_key", "miniopassword")

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
    con: duckdb.DuckDBPyConnection = duckdb.connect(
        database=DUCKDB_PATH, read_only=False
    )
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
        raw_json (str): Les données brutes en format JSON sous forme de chaîne.
        file_name (str): Nom du fichier dans lequel les données seront sauvegardées.
    """
    # Conversion en octets  et création d'un "fichier" virtuel en mémoire à partir des octets
    data_bytes: bytes = raw_json.encode("utf-8")
    data_stream: BytesIO = BytesIO(data_bytes)
    object_key: str = f"{today_date}/{file_name}"

    minio_client.put_object(
        BUCKET_NAME,
        object_key,
        data_stream,
        length=len(data_bytes),
        content_type="application/json",
    )

    logger.info(f"Fichier envoyé dans MinIO : {BUCKET_NAME}/{object_key}")
