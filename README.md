# 🚴 Sujet de travaux pratiques "Introduction à la data ingénierie 
Ce projet consiste à construire un pipeline pour la collecte, la transformation et l'analyse des données des systèmes de vélos en libre-service de plusieurs villes françaises : Paris, Nantes, Toulouse et Strasbourg.  
L'objectif est de consolider et aggréger ces données dans une base DuckDB pour permettre des analyses ultérieurs.

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

### **Exemple de requêtes analytiques** :

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

---

## 📥 **Sources des Données**

- [API Vélib' Métropole (Paris)](https://opendata.paris.fr/explore/dataset/velib-disponibilite-en-temps-reel/api/)  
- [API Nantes Métropole](https://data.nantesmetropole.fr/explore/dataset/244400404_stations-velos-libre-service-nantes-metropole-disponibilites/api/)  
- [API Toulouse Métropole](https://data.toulouse-metropole.fr/explore/dataset/api-velo-toulouse-temps-reel/api/)  
- [API Strasbourg](https://data.strasbourg.eu/explore/dataset/stations-velhop/api/)  
- [API Open Data Communes](https://geo.api.gouv.fr/communes)  

---

## 🌟 **Améliorations Futures**

1. Ajouter d'autres villes européennes pour étendre la couverture géographique.  
2. Mettre en place des dashboards interactifs avec **Tableau** ou **Dash**.  
3. Intégrer un orchestrateur plus avancé comme **Kestra** ou **Prefect**.  
4. Automatiser la surveillance des pipelines avec des métriques clés.  
5. Intégrer une API REST pour exposer les données consolidées à des applications tierces.

---

## 🎓 **Objectif Pédagogique**

Ce projet a été conçu pour permettre une introduction pratique à la data ingénierie. Les étudiants apprennent à :
- Collecter des données via des APIs.
- Construire un pipeline de données ETL avec des outils modernes.
- Travailler avec DuckDB pour des analyses rapides et efficaces.

---

![Diagramme Processus Final](images/image_2.png)
