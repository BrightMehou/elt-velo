import os
import logging
import streamlit as st
import duckdb
import plotly.express as px

# ----------------------------
# Configuration du logging
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
logger.info("Démarrage de l'application Streamlit.")

# ----------------------------
# Configuration de la page
# ----------------------------
st.set_page_config(
    page_title="Tableau de bord mobilité 🚲",
    layout="wide"
)

# ----------------------------
# Connexion DuckDB
# ----------------------------
con = duckdb.connect(
    database="data/duckdb/mobility_analysis.duckdb",
    read_only=True
)
logger.info("Connexion à DuckDB établie.")

# ----------------------------
# Titre et description
# ----------------------------
st.title("📊 Tableau de bord des stations de vélos 🚲")
st.markdown("""
Cette application présente :
1. Les données brutes des indicateurs  
2. Une carte interactive centrée sur Paris  
3. Les indicateurs clés sous forme de tableaux  
""")

# ----------------------------
# 1️⃣ Données brutes
# ----------------------------
with st.expander("🔍 Voir les données brutes"):
    # Brutes : Stations
    st.markdown("**Données brutes des stations**")
    df_stations = con.execute("SELECT * FROM DIM_STATION").df()
    st.dataframe(df_stations, use_container_width=True)

    # Brutes : Villes
    st.markdown("**Données brutes des villes**")
    df_cities = con.execute("SELECT * FROM DIM_CITY").df()
    st.dataframe(df_cities, use_container_width=True)

    # Brutes : États
    st.markdown("**Données brutes des états des stations**")
    df_states = con.execute("SELECT * FROM FACT_STATION_STATEMENT").df()
    st.dataframe(df_states, use_container_width=True)

# ----------------------------
# 2️⃣ Carte interactive
# ----------------------------
st.subheader("🗺️ Carte interactive des stations de vélos (centrée sur Paris)")

# Préparation des données de la carte
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
    SELECT MAX(CREATED_DATE) FROM FACT_STATION_STATEMENT WHERE STATION_ID = ds.ID
)
AND ds.LATITUDE IS NOT NULL
AND ds.LONGITUDE IS NOT NULL
"""
df_map = con.execute(query_map).df()

if df_map.empty:
    st.warning("Aucune donnée disponible pour la carte.")
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

# ----------------------------
# 3️⃣ Indicateurs clés
# ----------------------------
st.subheader("📈 Indicateurs clés")

# Indicateur 1
st.markdown("**1. Nombre d'emplacements disponibles pour les vélos par ville**")
query1 = """
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
df1 = con.execute(query1).df()
st.dataframe(df1, use_container_width=True)

# Indicateur 2
st.markdown("**2. Moyenne des vélos disponibles par station**")
query2 = """
    SELECT ds.NAME, ds.CODE, ds.ADDRESS, tmp.AVG_DOCK_AVAILABLE
    FROM DIM_STATION ds
    JOIN (
        SELECT STATION_ID, AVG(BICYCLE_AVAILABLE) AS AVG_DOCK_AVAILABLE
        FROM FACT_STATION_STATEMENT
        GROUP BY STATION_ID
    ) tmp ON ds.ID = tmp.STATION_ID
"""
df2 = con.execute(query2).df()
st.dataframe(df2, use_container_width=True)

# Indicateur 3
st.markdown("**3. Capacité totale par ville**")
query3 = """
    SELECT dc.name AS city_name,
           SUM(ds.CAPACITTY) AS total_capacity
    FROM DIM_STATION ds
    JOIN FACT_STATION_STATEMENT fss ON ds.ID = fss.STATION_ID
    JOIN DIM_CITY dc ON fss.CITY_ID = dc.ID
    GROUP BY dc.name
"""
df3 = con.execute(query3).df()
st.dataframe(df3, use_container_width=True)

# Fermeture de la connexion
con.close()

st.caption("Données issues du data warehouse local DuckDB.")
