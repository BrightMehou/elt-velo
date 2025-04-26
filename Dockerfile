FROM apache/airflow:2.10.5-python3.12

# Copier les fichiers du projet dans l'image Docker
COPY docker_requirements.txt /docker_requirements.txt
RUN pip install --no-cache-dir -r /docker_requirements.txt

ENV PYTHONPATH="/opt/airflow/:${PYTHONPATH}"