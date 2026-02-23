"""
Script d'ingestion des données en temps réel pour l'analyse de mobilité.

Fonctionnalités principales :
- Récupération des données vélo en temps réel des stations de vélo.
- Récupération des données des communes françaises via l'API geo.gouv.fr.
- Stockage des données dans PostgreSQL via je format JSONB.
"""

import logging
from datetime import datetime
from enum import StrEnum

import requests

from utils import store_json

logger = logging.getLogger(__name__)
today_date: str = datetime.now().strftime("%Y-%m-%d")


class CityUrl(StrEnum):
    PARIS = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/exports/json"
    NANTES = "https://data.nantesmetropole.fr/api/explore/v2.1/catalog/datasets/244400404_stations-velos-libre-service-nantes-metropole-disponibilites/exports/json"
    TOULOUSE = "https://data.toulouse-metropole.fr/api/explore/v2.1/catalog/datasets/api-velo-toulouse-temps-reel/exports/json?lang=fr&timezone=Europe%2FParis"
    STRASBOURG = "https://opendata.strasbourg.eu/api/explore/v2.1/catalog/datasets/stations-velhop/exports/json?lang=fr&timezone=Europe%2FBerlin"


URL_COMMUNES: str = "https://geo.api.gouv.fr/communes"


def get_realtime_bicycle_data() -> None:
    """
    Récupère les données en temps réel des stations de vélo.
    Si une ville échoue, crée un fichier JSON vide ([]) pour éviter un crash dbt.
    """
    for url in CityUrl:
        try:
            response = requests.get(url, timeout=20)
            url_name = url.name.lower()
            if response.status_code == 200 and response.text.strip():
                store_json(f"{url_name}_realtime_bicycle_data.json", response.text)
                logger.info(f"✅ Données {url.name} stockées avec succès")
            else:
                logger.warning(
                    f"⚠️ {url.name} indisponible (status: {response.status_code})"
                )
                store_json(f"{url_name}_realtime_bicycle_data.json", "[]")
        except Exception as e:
            logger.error(f"❌ Erreur pour {url.name}: {e}. Création fichier vide.")
            store_json(f"{url_name}_realtime_bicycle_data.json", "[]")


def get_commune_data() -> None:
    """Récupère les données des communes françaises et les stocke dans PostgreSQL."""
    try:
        response = requests.get(URL_COMMUNES, timeout=30)
        if response.status_code == 200 and response.text.strip():
            store_json("commune_data.json", response.text)
            logger.info("✅ Données communes stockées avec succès")
        else:
            logger.warning(
                f"⚠️ API communes indisponible (status: {response.status_code})"
            )
            store_json("commune_data.json", "[]")
    except Exception as e:
        logger.error(f"❌ Erreur récupération communes : {e}")
        store_json("commune_data.json", "[]")


def data_ingestion() -> None:
    """
    Ingérer les données en temps réel des vélos et des communes.
    """
    get_realtime_bicycle_data()
    get_commune_data()
