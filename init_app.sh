#!/bin/bash
set -e

python src/init_storage_layers.py

# Génération de la documentation dbt
dbt docs generate --project-dir src/transformation --profiles-dir src/transformation

# Lancement de Streamlit en arrière-plan
streamlit run src/ui.py --server.port 8501 --server.address 0.0.0.0 &

# Lancement du serveur dbt docs
dbt docs serve --project-dir src/transformation --profiles-dir src/transformation --port 8080 --host 0.0.0.0