import logging
import os
from datetime import datetime
from io import BytesIO

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
            serialize_data(response.text, f"{city}_realtime_bicycle_data.json")
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
        serialize_data(response.text, "commune_data.json")
        logger.info("Les données des communes ont été récupérées et envoyées à MinIO")
    else:
        logger.error(
            f"Impossible de récupérer les communes (status: {response.status_code})"
        )


def serialize_data(raw_json: str, file_name: str) -> None:
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


def data_ingestion() -> None:
    """
    Fonction principale pour ingérer les données en temps réel des vélos et des communes.
    """
    get_realtime_bicycle_data()
    get_commune_data()
