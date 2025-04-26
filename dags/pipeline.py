from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)
# Import des fonctions de traitement des données
from src.data_ingestion import get_realtime_bicycle_data, get_commune_data
from src.data_consolidation import (
    create_consolidate_tables,
    consolidate_city_data,
    consolidate_station_data,
    consolidate_station_statement_data,
)
from src.data_agregation import (
    create_agregate_tables,
    agregate_dim_city,
    agregate_dim_station,
    agregate_fact_station_statements,
)

# Configuration par défaut des tâches
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Définition du DAG
with DAG(
    dag_id="data_pipeline",
    default_args=default_args,
    description="Pipeline de traitement des données",
    schedule_interval="@daily",
    start_date=datetime(2024, 12, 1),
    catchup=False,
) as dag:

    # Tâches d'ingestion
    @task
    def task_get_bicycle_data():
        get_realtime_bicycle_data()

    @task
    def task_get_commune_data():
        get_commune_data()

    # Tâches de consolidation
    @task
    def task_create_consolidate_tables():
        create_consolidate_tables()

    @task
    def task_consolidate_city_data():
        consolidate_city_data()

    @task
    def task_consolidate_station_data():
        consolidate_station_data()

    @task
    def task_consolidate_station_statement_data():
        consolidate_station_statement_data()

    # Tâches d'agrégation
    @task
    def task_create_agregate_tables():
        create_agregate_tables()

    @task
    def task_agregate_dim_city():
        agregate_dim_city()

    @task
    def task_agregate_dim_station():
        agregate_dim_station()

    @task
    def task_agregate_fact_station_statements():
        agregate_fact_station_statements()

    # Définition des dépendances entre les tâches
    (
        [
            task_get_bicycle_data(),
            task_get_commune_data(),
            task_create_consolidate_tables(),
        ]
        >> task_consolidate_city_data()
        >> task_consolidate_station_data()
        >> task_consolidate_station_statement_data()
        >> task_create_agregate_tables()
        >> task_agregate_dim_city()
        >> task_agregate_dim_station()
        >> task_agregate_fact_station_statements()
    )
