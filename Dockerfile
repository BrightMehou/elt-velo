FROM apache/airflow:2.10.3-python3.12

# Copier les fichiers du projet dans l'image Docker
COPY docker_requirements.txt /docker_requirements.txt
RUN pip install --no-cache-dir -r /docker_requirements.txt

COPY dags/ /opt/airflow/dags/
COPY src/ /opt/airflow/src/
COPY data/ /opt/airflow/data/

USER airflow

ENV AIRFLOW_HOME=/opt/airflow

# Rajoute le pythonpath
ENV PYTHONPATH="/opt/airflow/:${PYTHONPATH}"