import json
import logging
import os
from datetime import datetime
from io import BytesIO

import pandas as pd
import requests
from minio import Minio

# Configuration du logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

today_date = datetime.now().strftime("%Y-%m-%d")
BUCKET_NAME = "bicycle-data"

minio_endpoint = os.getenv("minio_endpoint", "localhost:9000")
minio_access_key = os.getenv("minio_access_key", "minioadmin")
minio_secret_key = os.getenv("minio_secret_key", "miniopassword")

# Connexion au service MinIO local
minio_client = Minio(
    endpoint=minio_endpoint,
    access_key=minio_access_key,
    secret_key=minio_secret_key,
    secure=False,
)

if not minio_client.bucket_exists(BUCKET_NAME):
    minio_client.make_bucket(BUCKET_NAME)
    logger.info(f"Bucket '{BUCKET_NAME}' créé.")


def serialize_data_parquet(raw_json: str, file_name: str) -> None:
    """
    Convertit les données JSON en DataFrame aplanie,
    puis les enregistre en mémoire au format Parquet avant de les transférer dans MinIO.

    Args:
        raw_json (str): Chaîne JSON brute récupérée depuis une API.
        file_name (str): Nom de base du fichier (sans extension).
                         Le fichier final sera stocké dans MinIO sous :
                         YYYY-MM-DD/<file_name>.parquet
    """
    records = json.loads(raw_json)  # Parse le JSON en objet Python
    df = pd.json_normalize(records)  # Aplati le JSON en table tabulaire

    buffer = BytesIO()
    df.to_parquet(
        buffer, index=False, engine="pyarrow"
    )  # Conversion DataFrame → Parquet

    # ⚠️ Très important : remettre le curseur au début du buffer,
    # sinon MinIO lira à partir de la fin et enverra un fichier vide.
    buffer.seek(0)

    object_key = f"{today_date}/{file_name}.parquet"
    minio_client.put_object(
        BUCKET_NAME,
        object_key,
        buffer,
        length=len(buffer.getvalue()),
        content_type="application/octet-stream",
    )
    logger.info(f"Fichier Parquet envoyé dans MinIO : {BUCKET_NAME}/{object_key}")


def get_realtime_bicycle_data() -> None:
    """
    Récupère les données en temps réel des vélos pour Paris, Nantes, Toulouse et Strasbourg,
    et les stocke directement dans MinIO au format JSON.
    """
    urls = {
        "paris": "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/exports/json",
        "nantes": "https://data.nantesmetropole.fr/api/explore/v2.1/catalog/datasets/244400404_stations-velos-libre-service-nantes-metropole-disponibilites/exports/json",
        "toulouse": "https://data.toulouse-metropole.fr/api/explore/v2.1/catalog/datasets/api-velo-toulouse-temps-reel/exports/json?lang=fr&timezone=Europe%2FParis",
        "strasbourg": "https://opendata.strasbourg.eu/api/explore/v2.1/catalog/datasets/stations-velhop/exports/json?lang=fr&timezone=Europe%2FBerlin",
    }

    for city, url in urls.items():
        response = requests.get(url)
        if response.status_code == 200:
            serialize_data_parquet(response.text, f"{city}_realtime_bicycle_data")
        else:
            logger.error(
                f"Impossible de récupérer {city} (status: {response.status_code})"
            )


def get_commune_data() -> None:
    """
    Récupère les données des communes françaises et les stocke dans MinIO.
    """
    url = "https://geo.api.gouv.fr/communes"
    response = requests.get(url)

    if response.status_code == 200:
        serialize_data_parquet(response.text, "commune_data")
        logger.info("Les données des communes ont été récupérées et envoyées à MinIO")
    else:
        logger.error(
            f"Impossible de récupérer les communes (status: {response.status_code})"
        )


def data_ingestion() -> None:
    """
    Fonction principale pour ingérer les données en temps réel des vélos et des communes.
    """
    get_realtime_bicycle_data()
    get_commune_data()
