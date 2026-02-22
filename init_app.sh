#!/bin/bash
set -e

python src/init_db.py

dbt docs generate --project-dir dbt-transformation --profiles-dir dbt-transformation

streamlit run src/ui.py --server.port 8501 --server.address 0.0.0.0 &

dbt docs serve --project-dir dbt-transformation --profiles-dir dbt-transformation --port 8080 --host 0.0.0.0