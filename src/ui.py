"""
Tableau de bord Streamlit pour l’analyse de mobilité urbaine 🚲.

Fonctionnalités principales :
- Lancement du pipeline (ingestion + transformation) via un bouton.
- Carte interactive des stations avec Plotly.
- Indicateurs clés par ville et par station.
"""
import logging
import os
from collections.abc import Callable

import pandas as pd
import plotly.express as px
import streamlit as st
from plotly.graph_objects import Figure
from sqlalchemy import create_engine, text

from ingestion import data_ingestion
from utils import data_transformation

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", force=True
)

logger = logging.getLogger(__name__)
logger.info("Démarrage de l'application Streamlit.")

st.set_page_config(page_title="Tableau de bord mobilité", page_icon="🚲", layout="wide")
st.logo("🚲")

@st.cache_resource
def get_sql_engine() -> create_engine:
    DB_NAME: str = os.getenv("DB_NAME", "postgres")
    DB_USER: str = os.getenv("DB_USER", "postgres")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD", "postgres") 
    DB_HOST: str = os.getenv("DB_HOST", "localhost")
    DB_PORT: str = os.getenv("DB_PORT", "5432")
    DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(DB_URL)

engine = get_sql_engine()
if "loaded" not in st.session_state:
    st.session_state.loaded = False


st.title("📊 Tableau de bord des stations de vélos 🚲")

if st.button("🔄 Alimenter et afficher"):
    try:
        with st.status("🚀 Lancement du pipeline...", expanded=True) as status:
            steps: list[tuple[str, Callable[[], None]]] = [
                ("Ingestion des données", data_ingestion),
                ("Transformation des données", data_transformation),
            ]

            for label, func in steps:
                status.update(label=label, state="running")
                logger.info(label)
                func()
                status.update(label=f"✅ {label}", state="complete")

            status.update(label="✅ Pipeline terminé avec succès !", state="complete")
            st.success("Données alimentées et prêtes à l’affichage !")
            st.session_state.loaded = True

    except Exception as e:
        logger.exception("Erreur pipeline")
        st.error(f"❌ Échec du pipeline à l'étape '{label}' : {e}")
        st.session_state.loaded = False


if st.session_state.loaded:
    
    with engine.connect() as con:

        st.subheader("🗺️ Carte interactive des stations")
        query_map: str = """
        select * from map_station;
        """
        df_map: pd.DataFrame = pd.read_sql_query(text(query_map), con)
        if df_map.empty:
            st.warning("Aucune donnée pour la carte.")
            logger.warning("DataFrame pour la carte est vide.") 
        else:
            fig: Figure = px.scatter_map(
                df_map,
                lat="latitude",
                lon="longitude",
                hover_name="name",
                hover_data=[
                    "id",
                    "code",
                    "address",
                    "status",
                    "capacity",
                    "bicycle_docks_available",
                    "bicycle_available",
                    "last_statement_date",
                ],
                color="bicycle_available",
                color_continuous_scale=px.colors.sequential.Plasma,
                center=dict(lat=48.8566, lon=2.3522),  # Paris
                size_max=15,
                height=600,
                zoom=11,
            )
            st.plotly_chart(fig, config={"width": "stretch", "height": 600})
            logger.info("Données pour la carte chargées.")
        st.markdown("---")

        st.subheader("📈 Indicateurs clés")

        queries: list[tuple[str, str]] = [
            (
                "1. Emplacements dispo par ville",
                "select * from available_emplacement_by_city;",
            ),
            (
                "2. Moyenne vélos dispo par station",
                "select * from mean_bicycle_available_by_station;",
            ),
            ("3. Capacité totale par ville", "select * from total_capacity_by_city;"),
        ]
        
        for title, query in queries:
            st.markdown(f"**{title}**")
            df = pd.read_sql_query(text(query), con)
            st.dataframe(df, width="stretch")
            logger.info(f"Données pour '{title}' chargées.")

    st.caption("Données issues des API publiques des stations de vélos.")
else:
    st.info("🔘 Cliquez sur **Alimenter et afficher** pour charger les données.")
