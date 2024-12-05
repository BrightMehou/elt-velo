# 🚴 Sujet de travaux pratiques "Introduction à la data ingénierie 
Ce projet consiste à construire un pipeline pour la collecte, la transformation et l'analyse des données des systèmes de vélos en libre-service de plusieurs villes françaises : Paris, Nantes, Toulouse et Strasbourg.  
L'objectif est de consolider et aggréger ces données dans une base DuckDB pour permettre des analyses ultérieurs.

---

## 🎯 **Cahier des charges**

Le pipeline doit permettre de réaliser les actions suivantes :  

### **1. Collecter des données en temps réel**  
- **Description :**  
  Le pipeline doit être capable d'interroger les APIs des villes ciblées (Paris, Nantes, Toulouse, et Strasbourg) pour récupérer :  
  - La liste des stations de vélos en libre-service.  
  - Le statut en temps réel des vélos et des docks disponibles.  
- **Résultat attendu :**  
  Les données collectées doivent être enregistrées sous forme de fichiers JSON dans le dossier `data/raw_data` organisé par date.

---

### **2. Normaliser et consolider les données brutes**  
- **Description :**  
  Les données collectées depuis différentes APIs doivent être nettoyées et structurées afin d'être harmonisées dans un format commun. Les étapes incluent :  
  - Création d’identifiants uniques pour les villes et les stations.  
  - Normalisation des noms des colonnes et des types de données.  
  - Enrichissement des données avec des informations additionnelles (par exemple, des codes INSEE pour les villes).  
- **Résultat attendu :**  
  Les données consolidées doivent être chargées dans une base de données DuckDB, dans les tables suivante :  
  - `CONSOLIDATE_CITY` : Données sur les villes.  
  - `CONSOLIDATE_STATION` : Données sur les stations.  
  - `CONSOLIDATE_STATION_STATEMENT` : Données en temps réel sur les vélos et les docks disponibles.

---

### **3. Aggréger les données pour des analyses**  
- **Description :**  
  Les données consolidées doivent être aggrégées pour répondre aux questions analytiques suivantes :  
  - Nombre moyen de vélos disponibles par station.
  - Nombre de docks disponibles pour chaque ville.  
- **Résultat attendu :**  
  Les réponses aux requêtes analytiques doivent pouvoir être facilement exécutées depuis la base DuckDB via des scripts SQL ou Python (par exemple `query_duckdb.py`).

---

### **4. Automatiser les traitements via Airflow**  
- **Description :**  
  Les différentes étapes du pipeline (ingestion, consolidation, aggrégation) doivent être automatisées et orchestrées dans un workflow reproductible avec Apache Airflow.  
- **Résultat attendu :**  
  - Les tâches doivent être définies dans un DAG et exécutées dans l’ordre défini.  
  - Le pipeline doit se réexécuter automatiquement chaque jour à minuit.
  - Le service doit être dockerisé
 
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
├── dags/                     # Définitions des DAGs Airflow
│   └── pipeline.py           # Orchestration du pipeline
├── data/                     # Données utilisées par les processus
│   ├── duckdb/               # Base de données locale DuckDB
│   ├── raw_data/             # Données brutes classées par date
│   └── sql_statements/       # Requêtes SQL réutilisables
├── src/                      # Code source principal
│   ├── __init__.py           # Fichier d'initialisation du module
│   ├── data_aggregation.py   # Agrégation des données
│   ├── data_consolidation.py # Consolidation des données brutes
│   ├── data_ingestion.py     # Ingestion des données en temps réel
│   ├── main.py               # Point d'entrée principal
│   └── query_duckdb.py       # Requêtes analytiques DuckDB
├── images/                   # Images pour documentation et visualisation
├── Dockerfile                # Configuration Docker pour Airflow
├── docker-compose.yml        # Orchestration Docker Compose
├── docker_requirements.txt   # Dépendances Python spécifiques à Docker
├── requirements.txt          # Liste des dépendances Python
└── README.md                 # Documentation du projet
```

---

## 🚀 **Installation et Exécution**

### **Sans Orchestration Airflow**

1. **Cloner le dépôt :**  
   ```bash
   git clone https://github.com/kevinl75/polytech-de-101-2024-tp-subject.git
   cd polytech-de-101-2024-tp-subject
   ```

2. **Créer et activer un environnement virtuel :**  
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate  # Sous Windows : .venv\Scripts\activate
   ```

3. **Installer les dépendances :**  
   ```bash
   pip install -r requirements.txt
   ```

4. **Exécuter le script principal :**  
   ```bash
   python src/main.py
   ```

---

### **Avec Orchestration Airflow**

1. **Construire les images Docker :**  
   ```bash
   docker-compose build
   ```

2. **Initialiser la base de données Airflow :**  
   ```bash
   docker-compose run airflow-webserver airflow db init
   ```

3. **Créer un utilisateur administrateur pour Airflow :**  
   ```bash
   docker-compose run airflow-webserver airflow users create \
       --username admin \
       --password admin \
       --firstname Admin \
       --lastname User \
       --role Admin \
       --email admin@example.com
   ```

4. **Lancer les services Airflow :**  
   ```bash
   docker-compose up -d
   ```

5. **Accéder à l'interface Airflow :**  
   Rendez-vous sur [http://localhost:8080](http://localhost:8080).

---

## 📊 **Analyse des Résultats**

Exécutez le script pour interroger les données consolidées :  
```bash
python src/query_duckdb.py
```

vous devez obtenir les résultats des requêtes suivantes

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

Vous pouvez également modifirer le fichier src/query_duckdb.py pour exécuter vos propres requêtes ou télécharger l'exécutable suivant sur le site de Duckdb.

[Duckdb installation](https://duckdb.org/docs/installation/)

---

Ces objectifs définissent ce que votre pipeline doit accomplir de manière fonctionnelle, garantissant un produit final opérationnel, évolutif et fiable.
![Diagramme Processus Final](images/image_2.png)
