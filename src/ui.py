import logging

import duckdb
import plotly.express as px
import streamlit as st

from data_ingestion import data_ingestion
from data_transformation import data_transformation

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
logger.info("Démarrage de l'application Streamlit.")


st.set_page_config(page_title="Tableau de bord mobilité 🚲", layout="wide")


if "loaded" not in st.session_state:
    st.session_state.loaded = False

st.title("📊 Tableau de bord des stations de vélos 🚲")
st.markdown(
    "Cliquez sur **Alimenter et afficher** pour lancer le pipeline et visualiser les données."
)

if st.button("🔄 Alimenter et afficher"):
    try:
        with st.status("🚀 Lancement du pipeline...", expanded=True) as status:
            steps = [
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
    # Connexion DuckDB
    con = duckdb.connect("data/duckdb/mobility_analysis.duckdb", read_only=True)

    # 1️⃣ Données brutes
    with st.expander("🔍 Voir les données brutes"):
        st.markdown("**DIM_STATION**")
        st.dataframe(con.execute("select * from dim_station").df(), width="stretch")

        st.markdown("**DIM_CITY**")
        st.dataframe(con.execute("select * from dim_city").df(), width="stretch")

        st.markdown("**FACT_STATION_STATEMENT**")
        st.dataframe(
            con.execute("select * from fact_station_statement").df(),
            width="stretch",
        )

    # 2️⃣ Carte interactive
    st.subheader("🗺️ Carte interactive des stations")
    query_map = """
    select * from map_station;
    """
    df_map = con.execute(query_map).df()
    if df_map.empty:
        st.warning("Aucune donnée pour la carte.")
    else:
        fig = px.scatter_map(
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
        st.plotly_chart(fig, width="stretch", height=600)

    st.markdown("---")

    # 3️⃣ Indicateurs clés
    st.subheader("📈 Indicateurs clés")

    st.markdown("**1. Emplacements dispo par ville**")
    q1 = """
        select * from available_emplacement_by_city;
    """
    st.dataframe(con.execute(q1).df(), width="stretch")

    st.markdown("**2. Moyenne vélos dispo par station**")
    q2 = """
        select * from mean_bicycle_available_by_station;
    """
    st.dataframe(con.execute(q2).df(), width="stretch")

    st.markdown("**3. Capacité totale par ville**")
    q3 = """
        select * from total_capacity_by_city;
    """
    st.dataframe(con.execute(q3).df(), width="stretch")

    con.close()
    st.caption("Données issues des API publiques des stations de vélos.")
else:
    st.info("🔘 Cliquez sur **Alimenter et afficher** pour charger les données.")
