from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
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

# Configuration par défaut pour les tâches
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
    dag_id="data_pipeline",  # Nom unique pour le DAG
    default_args=default_args,  # Arguments par défaut
    description="Pipeline de traitement des données",
    schedule_interval="@daily",  # Planification (quotidienne ici)
    start_date=datetime(2024, 12, 1),  # Date de début
    catchup=False,  # Ne pas exécuter les dates passées
) as dag:

    # Tâches d'ingestion
    task_get_bicycle_data = PythonOperator(
        task_id="get_realtime_bicycle_data",
        python_callable=get_realtime_bicycle_data,
    )

    task_get_commune_data = PythonOperator(
        task_id="get_commune_data",
        python_callable=get_commune_data,
    )

    # Tâches de consolidation
    task_create_consolidate_tables = PythonOperator(
        task_id="create_consolidate_tables",
        python_callable=create_consolidate_tables,
    )

    task_consolidate_city_data = PythonOperator(
        task_id="consolidate_city_data",
        python_callable=consolidate_city_data,
    )

    task_consolidate_station_data = PythonOperator(
        task_id="consolidate_station_data",
        python_callable=consolidate_station_data,
    )

    task_consolidate_station_statement_data = PythonOperator(
        task_id="consolidate_station_statement_data",
        python_callable=consolidate_station_statement_data,
    )

    # Tâches d'agrégation
    task_create_agregate_tables = PythonOperator(
        task_id="create_agregate_tables",
        python_callable=create_agregate_tables,
    )

    task_agregate_dim_city = PythonOperator(
        task_id="agregate_dim_city",
        python_callable=agregate_dim_city,
    )

    task_agregate_dim_station = PythonOperator(
        task_id="agregate_dim_station",
        python_callable=agregate_dim_station,
    )

    task_agregate_fact_station_statements = PythonOperator(
        task_id="agregate_fact_station_statements",
        python_callable=agregate_fact_station_statements,
    )

    # Définir les dépendances entre les tâches
    (
        [task_get_bicycle_data, task_get_commune_data, task_create_consolidate_tables]
        >> task_consolidate_city_data
        >> task_consolidate_station_data
        >> task_consolidate_station_statement_data
        >> task_create_agregate_tables
        >> task_agregate_dim_city
        >> task_agregate_dim_station
        >> task_agregate_fact_station_statements
    )
