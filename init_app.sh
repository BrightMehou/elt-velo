#!/bin/bash
set -e

python src/init_db.py

dbt docs generate --project-dir src/transformation --profiles-dir src/transformation

streamlit run src/ui.py --server.port 8501 --server.address 0.0.0.0 &

dbt docs serve --project-dir src/transformation --profiles-dir src/transformation --port 8080 --host 0.0.0.0