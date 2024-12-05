# FROM python:3.12-slim

# WORKDIR /app

# RUN --mount=type=cache,target=/root/.cache/pip \
#     --mount=type=bind,source=requirements.txt,target=requirements.txt \
#     python -m pip install -r requirements.txt


# # Copy the source code into the container.
# COPY . .

# # Expose the port that the application listens on.
# EXPOSE 8000

# # Run the application.
# CMD ["python", "src/main.py"]

FROM apache/airflow:2.10.3-python3.12

# Copier les fichiers du projet dans l'image Docker
COPY docker_requirements.txt /docker_requirements.txt
RUN pip install --no-cache-dir -r /docker_requirements.txt

COPY dags/ /opt/airflow/dags/
COPY src/ /opt/airflow/src/
COPY data/ /opt/airflow/data/

USER airflow

ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONPATH="/opt/airflow/:${PYTHONPATH}"