import logging
import streamlit as st
import duckdb
import plotly.express as px

# On importe les fonctions du pipeline
from data_ingestion import get_realtime_bicycle_data, get_commune_data
from data_consolidation import (
    create_consolidate_tables,
    consolidate_city_data,
    consolidate_station_data,
)
from data_agregation import (
    create_agregate_tables,
    agregate_dim_city,
    agregate_dim_station,
    agregate_fact_station_statements,
)

# ----------------------------
# Setup logging
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
logger.info("Démarrage de l'application Streamlit.")

# ----------------------------
# Page config
# ----------------------------
st.set_page_config(
    page_title="Tableau de bord mobilité 🚲",
    layout="wide"
)

# ----------------------------
# Session state
# ----------------------------
if "loaded" not in st.session_state:
    st.session_state.loaded = False

# ----------------------------
# Title + Button
# ----------------------------
st.title("📊 Tableau de bord des stations de vélos 🚲")
st.markdown("Cliquez sur **Alimenter et afficher** pour lancer le pipeline et visualiser les données.")

if st.button("🔄 Alimenter et afficher"):
    progress = st.progress(0)
    
    step = 0

    steps = [
        ("Ingestion: get_realtime_bicycle_data", get_realtime_bicycle_data),
        ("Ingestion: get_commune_data", get_commune_data),
        ("Consolidation: create_consolidate_tables", create_consolidate_tables),
        ("Consolidation: consolidate_city_data", consolidate_city_data),
        ("Consolidation: consolidate_station_data", consolidate_station_data),
        ("Agrégation: create_agregate_tables", create_agregate_tables),
        ("Agrégation: agregate_dim_city", agregate_dim_city),
        ("Agrégation: agregate_dim_station", agregate_dim_station),
        ("Agrégation: agregate_fact_station_statements", agregate_fact_station_statements),
    ]
    total_steps = 9
    try:
        for index, (label, func) in enumerate(steps, start=1):
            logger.info(f"{index}/{total_steps} – {label}")
            func()
            step += 1
            progress.progress(int(step / total_steps * 100), text=label)

        st.success("✅ Données alimentées et prêtes à l’affichage !")
        st.session_state.loaded = True

    except Exception as e:
        logger.exception("Erreur pipeline")
        st.error(f"❌ Échec du pipeline à l'étape '{label}' : {e}")
        st.session_state.loaded = False

# ----------------------------
# Affichage conditionnel
# ----------------------------
if st.session_state.loaded:
    # Connexion DuckDB
    con = duckdb.connect("data/duckdb/mobility_analysis.duckdb", read_only=True)

    # 1️⃣ Données brutes
    with st.expander("🔍 Voir les données brutes"):
        st.markdown("**DIM_STATION**")
        st.dataframe(con.execute("SELECT * FROM DIM_STATION").df(), use_container_width=True)

        st.markdown("**DIM_CITY**")
        st.dataframe(con.execute("SELECT * FROM DIM_CITY").df(), use_container_width=True)

        st.markdown("**FACT_STATION_STATEMENT**")
        st.dataframe(con.execute("SELECT * FROM FACT_STATION_STATEMENT").df(), use_container_width=True)

    # 2️⃣ Carte interactive
    st.subheader("🗺️ Carte interactive des stations")
    query_map = """
    SELECT 
        ds.NAME,
        ds.LATITUDE,
        ds.LONGITUDE,
        fss.BICYCLE_AVAILABLE,
        ds.CAPACITTY,
        fss.CREATED_DATE
    FROM DIM_STATION ds
    JOIN FACT_STATION_STATEMENT fss ON ds.ID = fss.STATION_ID
    WHERE fss.CREATED_DATE = (
        SELECT MAX(CREATED_DATE)
        FROM FACT_STATION_STATEMENT
        WHERE STATION_ID = ds.ID
    )
    AND ds.LATITUDE IS NOT NULL
    AND ds.LONGITUDE IS NOT NULL
    """
    df_map = con.execute(query_map).df()
    if df_map.empty:
        st.warning("Aucune donnée pour la carte.")
    else:
        fig = px.scatter_map(
        df_map,
        lat="LATITUDE",
        lon="LONGITUDE",
        hover_name="NAME",
        hover_data=["BICYCLE_AVAILABLE", "CAPACITTY"],
        color="BICYCLE_AVAILABLE",
        size="BICYCLE_AVAILABLE",
        color_continuous_scale=px.colors.cyclical.IceFire,
        center=dict(lat=48.8566, lon=2.3522),  # Paris
        size_max=15,
        height=600,
        zoom=11
    )
        st.plotly_chart(fig, use_container_width=True, height=600)

    st.markdown("---")

    # 3️⃣ Indicateurs clés
    st.subheader("📈 Indicateurs clés")

    st.markdown("**1. Emplacements dispo par ville**")
    q1 = """
        SELECT dm.NAME, tmp.SUM_BICYCLE_DOCKS_AVAILABLE
        FROM DIM_CITY dm
        INNER JOIN (
            SELECT CITY_ID, SUM(BICYCLE_DOCKS_AVAILABLE) AS SUM_BICYCLE_DOCKS_AVAILABLE
            FROM FACT_STATION_STATEMENT
            WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION)
            GROUP BY CITY_ID
        ) tmp ON dm.ID = tmp.CITY_ID
        WHERE lower(dm.NAME) IN ('paris','nantes','strasbourg','toulouse')
    """
    st.dataframe(con.execute(q1).df(), use_container_width=True)

    st.markdown("**2. Moyenne vélos dispo par station**")
    q2 = """
        SELECT ds.NAME, ds.CODE, ds.ADDRESS, tmp.AVG_DOCK_AVAILABLE
        FROM DIM_STATION ds
        JOIN (
            SELECT STATION_ID, AVG(BICYCLE_AVAILABLE) AS AVG_DOCK_AVAILABLE
            FROM FACT_STATION_STATEMENT
            GROUP BY STATION_ID
        ) tmp ON ds.ID = tmp.STATION_ID
    """
    st.dataframe(con.execute(q2).df(), use_container_width=True)

    st.markdown("**3. Capacité totale par ville**")
    q3 = """
        SELECT dc.name AS city_name,
               SUM(ds.CAPACITTY) AS total_capacity
        FROM DIM_STATION ds
        JOIN FACT_STATION_STATEMENT fss ON ds.ID = fss.STATION_ID
        JOIN DIM_CITY dc ON fss.CITY_ID = dc.ID
        GROUP BY dc.name
    """
    st.dataframe(con.execute(q3).df(), use_container_width=True)

    con.close()
    st.caption("Données issues des API publiques des stations de vélos.")
else:
    st.info("🔘 Cliquez sur **Alimenter et afficher** pour charger les données.")
