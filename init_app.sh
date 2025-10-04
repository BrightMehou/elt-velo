#!/bin/bash
set -e

python src/init_storage_layers.py

# Génération de la documentation dbt
dbt docs generate --project-dir src/elt --profiles-dir src/elt

# Lancement de Streamlit en arrière-plan
streamlit run src/ui.py --server.port 8501 --server.address 0.0.0.0 &

# Lancement du serveur dbt docs
dbt docs serve --project-dir src/elt --profiles-dir src/elt --port 8080 --host 0.0.0.0