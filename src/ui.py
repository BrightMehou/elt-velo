"""
Tableau de bord Streamlit pour l’analyse de mobilité urbaine 🚲.

Fonctionnalités principales :
- Lancement du pipeline (ingestion + transformation) via un bouton.
- Carte interactive des stations avec Plotly.
- Indicateurs clés par ville et par station.
"""

import logging
from collections.abc import Callable

import duckdb
import plotly.express as px
import streamlit as st
from pandas import DataFrame
from plotly.graph_objects import Figure

from ingestion import data_ingestion
from utils import DUCKDB_PATH, data_transformation

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger: logging.Logger = logging.getLogger(__name__)
logger.info("Démarrage de l'application Streamlit.")

st.set_page_config(page_title="Tableau de bord mobilité 🚲", layout="wide")


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
    con: duckdb.DuckDBPyConnection = duckdb.connect(DUCKDB_PATH, read_only=True)

    st.subheader("🗺️ Carte interactive des stations")
    query_map: str = """
    select * from map_station;
    """
    df_map: DataFrame = con.execute(query_map).df()
    if df_map.empty:
        st.warning("Aucune donnée pour la carte.")
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
        df = con.execute(query).df()
        st.dataframe(df, width="stretch")

    con.close()
    st.caption("Données issues des API publiques des stations de vélos.")
else:
    st.info("🔘 Cliquez sur **Alimenter et afficher** pour charger les données.")
