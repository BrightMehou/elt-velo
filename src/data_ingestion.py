import os
from datetime import datetime
import requests


def get_paris_realtime_bicycle_data() -> None:
    """
    Récupère les données en temps réel des stations de vélos à Paris 
    depuis l'API OpenData Paris et les sauvegarde sous forme de fichier JSON.

    Les données sont sauvegardées dans le dossier : `data/raw_data/YYYY-MM-DD`.
    """
    url = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/exports/json"
    
    response = requests.request("GET", url)
    
    serialize_data(response.text, "paris_realtime_bicycle_data.json")

def get_nante_realtime_bicycle_data() -> None:
    """
    Récupère les données en temps réel des stations de vélos à Nantes 
    depuis l'API Nantes Métropole et les sauvegarde sous forme de fichier JSON.

    Les données sont sauvegardées dans le dossier : `data/raw_data/YYYY-MM-DD`.
    """
    url = "https://data.nantesmetropole.fr/api/explore/v2.1/catalog/datasets/244400404_stations-velos-libre-service-nantes-metropole-disponibilites/exports/json"
    
    response = requests.request("GET", url)
    
    serialize_data(response.text, "nantes_realtime_bicycle_data.json")

def get_toulouse_realtime_bicycle_data() -> None:
    """
    Récupère les données en temps réel des stations de vélos à Toulouse 
    depuis l'API Toulouse Métropole et les sauvegarde sous forme de fichier JSON.

    Les données sont sauvegardées dans le dossier : `data/raw_data/YYYY-MM-DD`.
    """
    url = "https://data.toulouse-metropole.fr/api/explore/v2.1/catalog/datasets/api-velo-toulouse-temps-reel/exports/json?lang=fr&timezone=Europe%2FParis"
    
    response = requests.request("GET", url)
    
    serialize_data(response.text, "toulouse_realtime_bicycle_data.json")

def get_commune_data() -> None:
    """
    Récupère les données des communes françaises depuis l'API GeoAPI et 
    les sauvegarde sous forme de fichier JSON.

    Les données sont sauvegardées dans le dossier : `data/raw_data/YYYY-MM-DD`.
    """
    url = "https://geo.api.gouv.fr/communes"
    
    response = requests.request("GET", url)

    serialize_data(response.text, "commune_data.json")

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
