from datetime import datetime
import logging
from minio import Minio
import os
from io import BytesIO

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

BUCKET_NAME = "bicycle-data"
today_date = datetime.now().strftime("%Y-%m-%d")
minio_endpoint = os.getenv("minio_endpoint", "localhost:9000")
minio_access_key = os.getenv("minio_access_key", "minioadmin")
minio_secret_key = os.getenv("minio_secret_key", "miniopassword")

minio_client = Minio(
    endpoint=minio_endpoint,
    access_key=minio_access_key,
    secret_key=minio_secret_key,
    secure=False,
)

def init_minio_bucket() -> None:
    f"""Crée le bucket {BUCKET_NAME} dans MinIO s'il n'existe pas.
    """
    if not minio_client.bucket_exists(BUCKET_NAME):
        minio_client.make_bucket(BUCKET_NAME)
        logger.info(f"Bucket '{BUCKET_NAME}' créé.")

def store_json(raw_json: str, file_name: str) -> None:
    f"""
    Envoie les données JSON dans MinIO dans le bucket {BUCKET_NAME}.
    Structure du chemin : YYYY-MM-DD/nom_fichier.json

    Args:
        raw_json (str): Les données brutes en format JSON sous forme de chaîne.
        file_name (str): Nom du fichier dans lequel les données seront sauvegardées.
    """
    # Conversion en octets  et création d'un "fichier" virtuel en mémoire à partir des octets
    data_bytes = raw_json.encode("utf-8")
    data_stream = BytesIO(data_bytes)

    object_key = f"{today_date}/{file_name}"

    minio_client.put_object(
        BUCKET_NAME,
        object_key,
        data_stream,
        length=len(data_bytes),
        content_type="application/json",
    )

    logger.info(f"Fichier envoyé dans MinIO : {BUCKET_NAME}/{object_key}")
