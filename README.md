# Sujet de travaux pratiques "Introduction à la data ingénierie"
Ce projet vise à créer un pipeline pour collecter, transformer et analyser les données des systèmes de vélos en libre-service de plusieurs villes françaises, notamment Paris, Nantes, Toulouse et Strasbourg. Il utilise les API ouvertes des villes pour obtenir des données en temps réel sur les stations et les vélos disponibles, les consolide et les aggrèges dans une base de données DuckDB pour une des analyses ultérieurs.

## Explication du code existant

Le projet est découpé en 3 parties :

1. Un fichier python pour récupérer et stocker les données dans des fichiers localement

2. Un fichier python pour consolider les données et faire un premier load dans une base de données type data-warehouse

3. Un fichier python pour agréger les données et créer une modélisation de type dimensionnelle


## Structure du projet
```graphql
├── dags/                     # Contient les fichiers de définition des DAGs
│   └── pipeline.py           # Pipeline Airflow
├── data/                     # Données utilisées par les DAGs
│   ├── duckdb/               # Base de données DuckDB locale
│   ├── raw_data/             # Données brutes, organisées par date
│   └── sql_statements/       # Scripts SQL réutilisables
├── src/                      # Code source pour les différents processus de données
│   ├── __init__.py           # Indique que le dossier est un module Python
│   ├── data_agregation.py    # Script pour l'agrégation des données
│   ├── data_consolidation.py # Script pour la consolidation des données
│   ├── data_ingestion.py     # Script pour l'ingestion des données en temps réel
│   ├── main.py               # Point d'entrée principal pour le traitement des données
│   └── query_duckdb.py       # Script pour interroger la base de données DuckDB
├── images/                   # Images ou visualisations générées par les tâches
├── Dockerfile                # Configuration Docker pour Airflow
├── docker-compose.yml        # Configuration Docker Compose pour orchestrer les services
├── docker_requirements.txt   # Dépendances Python spécifiques au Docker
├── requirements.txt          # Dépendances Python du projet
└── Dockerfile                # Configuration Docker pour Airflow
```


### Installation et exécution sans orchestration Airflow

Pour faire fonctionner le projet sans l'orchestration Airflow, suivez ces étapes :

1. **Cloner le dépôt** :
    ```bash
    git clone https://github.com/kevinl75/polytech-de-101-2024-tp-subject.git
    cd polytech-de-101-2024-tp-subject
    ```

2. **Créer et activer un environnement virtuel** :
    ```bash
    python3 -m venv .venv
    source .venv/bin/activate
    ```

3. **Installer les dépendances** :
    ```bash
    pip install -r requirements.txt
    ```

4. **Exécuter le script principal** :
    ```bash
    python src/main.py
    ```

### Installation et exécution avec orchestration Airflow

Pour utiliser le projet avec l'orchestration Airflow, suivez ces étapes :

1. **Construire l'image Docker** :
    ```bash
    docker-compose build
    ```

2. **Initialiser la base de données Airflow** :
    ```bash
    docker-compose run airflow-webserver airflow db init
    ```

3. **Créer un utilisateur administrateur pour Airflow** :
    ```bash
    docker-compose run airflow-webserver airflow users create \
        --username admin \
        --password admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com
    ```

4. **Démarrer les services Airflow** :
    ```bash
    docker-compose up -d
    ```

5. **Accéder à l'interface web Airflow** :
    Ouvrez votre navigateur et rendez-vous sur [http://localhost:8080](http://localhost:8080).

### Tester les résultats du pipeline

Pour tester les résultats du pipeline, exécutez la commande suivante :

```bash
python src/query_duckdb.py
```

Vous obtiendrez les résultats des requêtes suivantes :

```sql
-- Nombre d'emplacements disponibles de vélos dans une ville
SELECT dm.NAME, tmp.SUM_BICYCLE_DOCKS_AVAILABLE
FROM DIM_CITY dm INNER JOIN (
    SELECT CITY_ID, SUM(BICYCLE_DOCKS_AVAILABLE) AS SUM_BICYCLE_DOCKS_AVAILABLE
    FROM FACT_STATION_STATEMENT
    WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION)
    GROUP BY CITY_ID
) tmp ON dm.ID = tmp.CITY_ID
WHERE lower(dm.NAME) IN ('paris', 'nantes', 'vincennes', 'toulouse');

-- Nombre de vélos disponibles en moyenne dans chaque station
SELECT ds.name, ds.code, ds.address, tmp.avg_dock_available
FROM DIM_STATION ds JOIN (
    SELECT station_id, AVG(BICYCLE_AVAILABLE) AS avg_dock_available
    FROM FACT_STATION_STATEMENT
    GROUP BY station_id
) AS tmp ON ds.id = tmp.station_id;
```

## Sources des données 

Le but de ce TP est d'enrichir ce pipeline avec des données provenant d'autres villes. Les sources de données disponibles sont :

- [Open data Paris](https://opendata.paris.fr/explore/dataset/velib-disponibilite-en-temps-reel/api/)

- [Open data Nantes](https://data.nantesmetropole.fr/explore/dataset/244400404_stations-velos-libre-service-nantes-metropole-disponibilites/api/)

- [Open data Toulouse](https://data.toulouse-metropole.fr/explore/dataset/api-velo-toulouse-temps-reel/api/)

- [Open data Strasbourg](https://data.strasbourg.eu/explore/dataset/stations-velhop/api/)

- [Open data communes](https://geo.api.gouv.fr/communes)

![Process final](images/image_2.png)
