import streamlit as st
import duckdb
import pandas as pd

# Connexion à la base
con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=True)

st.title("📊 Tableau de bord des stations de vélos 🚲")

st.sidebar.title("📌 Sélection des indicateurs")

# 1️⃣ Nb d'emplacements disponibles de vélos dans certaines villes
if st.sidebar.checkbox("Nb d'emplacements disponibles de vélos par ville", value=True):
    st.subheader("Nb d'emplacements disponibles de vélos dans une ville")
    requete = """
        SELECT dm.NAME, tmp.SUM_BICYCLE_DOCKS_AVAILABLE 
        FROM DIM_CITY dm 
        INNER JOIN (
            SELECT CITY_ID, SUM(BICYCLE_DOCKS_AVAILABLE) AS SUM_BICYCLE_DOCKS_AVAILABLE 
            FROM FACT_STATION_STATEMENT 
            WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION)
            GROUP BY CITY_ID
        ) tmp ON dm.ID = tmp.CITY_ID 
        WHERE lower(dm.NAME) in ('paris', 'nantes', 'vincennes', 'toulouse', 'strasbourg')
    """
    df = con.execute(requete).df()
    st.dataframe(df)
    st.bar_chart(df.set_index("NAME"))

# 2️⃣ Nb de vélos disponibles en moyenne dans chaque station
if st.sidebar.checkbox("Nb de vélos disponibles en moyenne par station"):
    st.subheader("Nb de vélos disponibles en moyenne dans chaque station")
    requete = """
        SELECT ds.name, ds.code, ds.address, tmp.avg_dock_available
        FROM DIM_STATION ds 
        JOIN (
            SELECT station_id, AVG(BICYCLE_AVAILABLE) AS avg_dock_available 
            FROM FACT_STATION_STATEMENT 
            GROUP BY station_id
        ) AS tmp ON ds.id = tmp.station_id
    """
    df = con.execute(requete).df()
    st.dataframe(df)

# 3️⃣ Taux d’occupation moyen des stations par ville
if st.sidebar.checkbox("Taux d’occupation moyen des stations par ville"):
    st.subheader("Taux d’occupation moyen des stations par ville")
    requete = """
        SELECT dc.name AS city_name,
               ROUND(SUM(fss.BICYCLE_AVAILABLE) * 1.0 / SUM(ds.CAPACITTY), 3) AS avg_occupancy_rate
        FROM FACT_STATION_STATEMENT fss
        JOIN DIM_STATION ds ON fss.STATION_ID = ds.ID
        JOIN DIM_CITY dc ON fss.CITY_ID = dc.ID
        GROUP BY dc.name
        ORDER BY avg_occupancy_rate DESC
    """
    df = con.execute(requete).df()
    st.dataframe(df)
    st.bar_chart(df.set_index("city_name"))

# 4️⃣ Évolution du nombre total de vélos disponibles par jour
if st.sidebar.checkbox("Évolution du nombre total de vélos par jour"):
    st.subheader("Évolution du nombre total de vélos disponibles par jour")
    requete = """
        SELECT CREATED_DATE,
               SUM(BICYCLE_AVAILABLE) AS total_bicycles_available
        FROM FACT_STATION_STATEMENT
        GROUP BY CREATED_DATE
        ORDER BY CREATED_DATE
    """
    df = con.execute(requete).df()
    st.line_chart(df.rename(columns={"CREATED_DATE": "index"}).set_index("index"))

# 5️⃣ Répartition des stations par statut
if st.sidebar.checkbox("Répartition des stations par statut"):
    st.subheader("Répartition des stations par statut")
    requete = """
        SELECT STATUS, COUNT(*) AS nb_stations
        FROM DIM_STATION
        GROUP BY STATUS
    """
    df = con.execute(requete).df()
    st.dataframe(df)
    st.bar_chart(df.set_index("STATUS"))

# 6️⃣ Capacité totale par ville
if st.sidebar.checkbox("Capacité totale par ville"):
    st.subheader("Capacité totale par ville")
    requete = """
        SELECT dc.name AS city_name,
               SUM(ds.CAPACITTY) AS total_capacity
        FROM DIM_STATION ds
        JOIN FACT_STATION_STATEMENT fss ON ds.ID = fss.STATION_ID
        JOIN DIM_CITY dc ON fss.CITY_ID = dc.ID
        GROUP BY dc.name
    """
    df = con.execute(requete).df()
    st.dataframe(df)
    st.bar_chart(df.set_index("city_name"))

# 7️⃣ Taux de disponibilité moyen des bornes par ville
if st.sidebar.checkbox("Taux de disponibilité moyen des bornes par ville"):
    st.subheader("Taux de disponibilité moyen des bornes par ville")
    requete = """
        SELECT dc.name AS city_name,
               ROUND(SUM(fss.BICYCLE_DOCKS_AVAILABLE) * 1.0 / SUM(ds.CAPACITTY), 3) AS avg_dock_availability_rate
        FROM FACT_STATION_STATEMENT fss
        JOIN DIM_STATION ds ON fss.STATION_ID = ds.ID
        JOIN DIM_CITY dc ON fss.CITY_ID = dc.ID
        GROUP BY dc.name
    """
    df = con.execute(requete).df()
    st.dataframe(df)
    st.bar_chart(df.set_index("city_name"))

# Fermeture connexion
con.close()
