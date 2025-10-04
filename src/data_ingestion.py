"""
Script d’ingestion de données temps réel pour les vélos et les communes françaises,
avec stockage dans MinIO.

Fonctionnalités principales :
- Récupération des données temps réel pour les vélos (Paris, Nantes, Toulouse, Strasbourg).
- Récupération des données des communes françaises via l’API gouvernementale.
- Sérialisation et stockage des données brutes JSON dans MinIO
  avec une structure de chemin : YYYY-MM-DD/nom_fichier.json.
"""

import logging
from datetime import datetime

import requests

from utils import store_json

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger: logging.Logger = logging.getLogger(__name__)
today_date: str = datetime.now().strftime("%Y-%m-%d")


def get_realtime_bicycle_data() -> None:
    """
    Récupère les données en temps réel des vélos pour Paris, Nantes, Toulouse et Strasbourg.
    Si une ville échoue, crée un fichier JSON vide ([]) pour éviter un crash dbt.
    """
    urls: dict[str, str] = {
        "paris": "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/exports/json",
        "nantes": "https://data.nantesmetropole.fr/api/explore/v2.1/catalog/datasets/244400404_stations-velos-libre-service-nantes-metropole-disponibilites/exports/json",
        "toulouse": "https://data.toulouse-metropole.fr/api/explore/v2.1/catalog/datasets/api-velo-toulouse-temps-reel/exports/json?lang=fr&timezone=Europe%2FParis",
        "strasbourg": "https://opendata.strasbourg.eu/api/explore/v2.1/catalog/datasets/stations-velhop/exports/json?lang=fr&timezone=Europe%2FBerlin",
    }

    for city, url in urls.items():
        try:
            response: requests.Response = requests.get(url, timeout=20)
            if response.status_code == 200 and response.text.strip():
                store_json(response.text, f"{city}_realtime_bicycle_data.json")
                logger.info(f"✅ Données {city} stockées avec succès")
            else:
                logger.warning(
                    f"⚠️ {city} indisponible (status: {response.status_code}), création fichier vide."
                )
                store_json("[]", f"{city}_realtime_bicycle_data.json")
        except Exception as e:
            logger.error(f"❌ Erreur pour {city}: {e}. Création fichier vide.")
            store_json("[]", f"{city}_realtime_bicycle_data.json")


def get_commune_data() -> None:
    """Récupère les données des communes françaises et les stocke dans MinIO avec fallback JSON vide."""
    url: str = "https://geo.api.gouv.fr/communes"
    try:
        response: requests.Response = requests.get(url, timeout=30)
        if response.status_code == 200 and response.text.strip():
            store_json(response.text, "commune_data.json")
            logger.info("✅ Données communes stockées avec succès")
        else:
            logger.warning(
                f"⚠️ API communes indisponible (status: {response.status_code}), création fichier vide."
            )
            store_json("[]", "commune_data.json")
    except Exception as e:
        logger.error(f"❌ Erreur récupération communes : {e}")
        store_json("[]", "commune_data.json")


def data_ingestion() -> None:
    """
    Fonction principale pour ingérer les données en temps réel des vélos et des communes.
    """
    get_realtime_bicycle_data()
    get_commune_data()
