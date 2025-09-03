## 🚴 ETL-Velo

Ce projet propose la mise en place d’un pipeline simple pour collecter, transformer et analyser les données des systèmes de vélos en libre-service de Paris, Nantes, Toulouse et Strasbourg.
Les données sont stockées dans MinIO (data lake), transformées à l’aide de DBT (Data Build Tool) pour assurer la qualité, la modularité et la traçabilité des modèles de données, puis consolidées dans DuckDB (data warehouse). Enfin, elles sont présentées via Streamlit pour faciliter l’exploration et la visualisation des résultats.

---

## 📥 **Sources des Données**

- [API Paris](https://opendata.paris.fr/explore/dataset/velib-disponibilite-en-temps-reel/api/)  
- [API Nantes](https://data.nantesmetropole.fr/explore/dataset/244400404_stations-velos-libre-service-nantes-metropole-disponibilites/api/)  
- [API Toulouse](https://data.toulouse-metropole.fr/explore/dataset/api-velo-toulouse-temps-reel/api/)  
- [API Strasbourg](https://data.strasbourg.eu/explore/dataset/stations-velhop/api/)  
- [API Open Data Communes](https://geo.api.gouv.fr/communes)  

---

## 🗂️ **Structure du Projet**

```plaintext
├── data/                      # Données utilisées par les processus
│   └── duckdb/                # Base de données locale DuckDB
├── src/                       # Code source principal
│   ├── elt/                   # Projet DBT pour la transformation des données
│   ├── sql_statements/        # Requêtes SQL réutilisables
│   ├── data_ingestion.py      # Ingestion des données en temps réel
│   ├── data_transformation.py # Transformation des données brutes
│   ├── init_db.py             # Fichier d'initialisation de la base de données
│   └── ui.py                  # Interface utilisateur
├── docker-compose.yml         # Orchestration Docker Compose
├── Dockerfile                 # Configuration Docker
├── poetry.lock                # Verrouillage des dépendances Poetry
├── pyproject.toml             # Configuration du projet Poetry
└── README.md                  # Documentation du projet
```

---

## ⚙️ **Workflow du Projet**

### **1. Ingestion des données**
**Objectif** : Récupérer des données brutes depuis des sources externes.
#### Étapes : 
Dans le fichier Python `data_ingestion.py`
- **`get_realtime_bicycle_data`** : 
  - Récupère les données en temps réel sur les vélos disponibles des villes (Paris, Nantes, Toulouse, Strasbourg).
- **`get_commune_data`** : 
  - Récupère des données sur les communes.

#### Produits :
- Les données brutes sont enregistrées dans les fichiers Parquet dans le bucket dédié.



### **2. Transformation des données avec DBT**  
**Objectif** : Organiser, nettoyer et structurer les données brutes issues du data lake pour les rendre exploitables.

#### Étapes :  
La transformation des données est orchestrée via **DBT**, selon une architecture modulaire :

- 📁 **Staging**  
  - Création de tables temporaires à partir des fichiers bruts stockés dans **MinIO**.  
  - Ces modèles permettent de normaliser les formats et de préparer les données pour les étapes suivantes.

- 📁 **Consolidate**  
  - Construction de tables consolidées, alimentées en **mode incrémental**, pour intégrer les nouvelles données sans retraiter l’ensemble du dataset.  
  - Les données des communes et des stations sont nettoyées, enrichies et structurées pour l’analyse.

#### Produits :  
- Les tables consolidées sont stockées dans **DuckDB** et servent de base aux modèles analytiques et aux vues agrégées.

---

### **3. Modélisation analytique et agrégation**  
**Objectif** : Synthétiser les données consolidées pour produire des modèles analytiques et des vues prêtes à l’exploration.

#### Étapes :  
La modélisation suit une logique en étoile et se décompose en deux niveaux :

- 📁 **Star_model**  
  - Création des **tables dimensionnelles** (ex. : `dim_city`, `dim_station`) et de la **table factuelle** (`fact_station_statement`) en associant les données consolidées.  
  - Ces modèles facilitent les jointures et les analyses multi-axes.

- 📁 **Analytics**  
  - Génération de **vues analytiques** prêtes à être exposées dans **Streamlit**.  
  - Ces vues permettent d’explorer les métriques clés et les tendances du système de vélos en libre-service.

#### Produits :  
- Les vues finales sont stockées dans **DuckDB** et intégrées à l’interface Streamlit pour la visualisation interactive.

---

## 🚀 **Installation et Exécution**

1. **Cloner le dépôt :**  
   ```bash
   git clone https://github.com/BrightMehou/etl-velo.git
   cd etl-velo
   ```

2. **Installer Docker** : 
   Si Docker n'est pas encore installé : [Docker installation](https://www.docker.com/)

3. **Construire les images Docker et lancer les containeurs :**  
   ```bash
   docker-compose up -d
   ```

4. **Accéder à l'interface streamlit :**  
   Rendez-vous sur [http://localhost:8501](http://localhost:8501) 

---

## 📊 **Analyse des données**


Vous devriez obtenir les résultats des requêtes suivantes.

#### 1. Nombre d'emplacements disponibles pour les vélos dans une ville :
```sql
SELECT dm.NAME, tmp.SUM_BICYCLE_DOCKS_AVAILABLE
FROM DIM_CITY dm
INNER JOIN (
    SELECT CITY_ID, SUM(BICYCLE_DOCKS_AVAILABLE) AS SUM_BICYCLE_DOCKS_AVAILABLE
    FROM FACT_STATION_STATEMENT
    WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION)
    GROUP BY CITY_ID
) tmp ON dm.ID = tmp.CITY_ID
WHERE lower(dm.NAME) IN ('paris', 'nantes', 'strasbourg', 'toulouse');
```

#### 2. Moyenne des vélos disponibles par station :
```sql
SELECT ds.NAME, ds.CODE, ds.ADDRESS, tmp.AVG_DOCK_AVAILABLE
FROM DIM_STATION ds
JOIN (
    SELECT STATION_ID, AVG(BICYCLE_AVAILABLE) AS AVG_DOCK_AVAILABLE
    FROM FACT_STATION_STATEMENT
    GROUP BY STATION_ID
) tmp ON ds.ID = tmp.STATION_ID;
```