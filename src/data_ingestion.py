import os
from datetime import datetime
import requests


def get_realtime_bicycle_data() -> None:
    """
    Récupération des données en temps réel des vélos en libre-service pour Paris, Nantes, Toulouse et Strasbourg.

    Cette fonction :
    1. Télécharge les données en temps réel depuis les API ouvertes des quatre villes.
    2. Sauvegarde les données JSON dans des fichiers locaux spécifiques pour chaque ville.
    3. Affiche un message pour indiquer si les données ont été récupérées avec succès ou si une erreur s'est produite.

    Les fichiers sont enregistrés dans un répertoire local au format : `nom_ville_realtime_bicycle_data.json`.
    """

    # URLs des API pour les données des vélos en libre-service et noms de villes correspondant aux URLs
    urls = [
        (
            "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/exports/json",
            "paris",
        ),
        (
            "https://data.nantesmetropole.fr/api/explore/v2.1/catalog/datasets/244400404_stations-velos-libre-service-nantes-metropole-disponibilites/exports/json",
            "nantes",
        ),
        (
            "https://data.toulouse-metropole.fr/api/explore/v2.1/catalog/datasets/api-velo-toulouse-temps-reel/exports/json?lang=fr&timezone=Europe%2FParis",
            "toulouse",
        ),
        (
            "https://opendata.strasbourg.eu/api/explore/v2.1/catalog/datasets/stations-velhop/exports/json?lang=fr&timezone=Europe%2FBerlin",
            "strasbourg",
        ),
    ]

    # Liste des noms de villes correspondant aux URLs
    # cities = ["paris", "nantes", "toulouse", "strasbourg"]

    for url, city in urls:
        response = requests.request("GET", url)
        if response.status_code == 200:
            serialize_data(response.text, f"{city}_realtime_bicycle_data.json")
            print(f"Les données de {city} ont été récuperées")
        else:
            print(
                f"Error: Impossible de récuper les données de {city} (status code: {response.status_code})"
            )


def get_commune_data() -> None:
    """
    Récupère les données des communes françaises depuis l'API GeoAPI et
    les sauvegarde sous forme de fichier JSON.

    Les données sont sauvegardées dans le dossier : `data/raw_data/YYYY-MM-DD`.
    """
    url = "https://geo.api.gouv.fr/communes"

    response = requests.request("GET", url)

    if response.status_code == 200:
        serialize_data(response.text, "commune_data.json")
        print(f"Les données des communes ont été récuperées")
    else:
        print(
            f"Error: Impossible de récuper les données des communes (status code: {response.status_code})"
        )


def serialize_data(raw_json: str, file_name: str) -> None:
    """
    Sauvegarde les données brutes JSON dans un fichier sous le répertoire
    `data/raw_data/YYYY-MM-DD`.

    Args:
        raw_json (str): Les données brutes en format JSON sous forme de chaîne.
        file_name (str): Nom du fichier dans lequel les données seront sauvegardées.
    """

    today_date = datetime.now().strftime("%Y-%m-%d")

    if not os.path.exists(f"data/raw_data/{today_date}"):
        os.makedirs(f"data/raw_data/{today_date}")

    with open(f"data/raw_data/{today_date}/{file_name}", "w") as fd:
        fd.write(raw_json)
