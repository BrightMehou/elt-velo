import duckdb

# Connexion
con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=True)

# 1️⃣ Nb d'emplacements disponibles de vélos dans une ville
print("-- Nb d'emplacements disponibles de vélos dans une ville")
requete = """
    SELECT dm.NAME, tmp.SUM_BICYCLE_DOCKS_AVAILABLE 
    FROM DIM_CITY dm 
    INNER JOIN (
        SELECT CITY_ID, SUM(BICYCLE_DOCKS_AVAILABLE) AS SUM_BICYCLE_DOCKS_AVAILABLE 
        FROM FACT_STATION_STATEMENT 
        WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION)
        GROUP BY CITY_ID
    ) tmp ON dm.ID = tmp.CITY_ID 
    WHERE lower(dm.NAME) in ('paris', 'nantes', 'vincennes', 'toulouse', 'strasbourg');
"""
con.sql(requete).show()

# 2️⃣ Nb de vélos disponibles en moyenne dans chaque station
print("-- Nb de vélos disponibles en moyenne dans chaque station")
requete = """
    SELECT ds.name, ds.code, ds.address, tmp.avg_dock_available
    FROM DIM_STATION ds 
    JOIN (
        SELECT station_id, AVG(BICYCLE_AVAILABLE) AS avg_dock_available 
        FROM FACT_STATION_STATEMENT 
        GROUP BY station_id
    ) AS tmp ON ds.id = tmp.station_id;
"""
con.sql(requete).show()

# 3️⃣ Taux d’occupation moyen des stations par ville
print("-- Taux d’occupation moyen des stations par ville")
requete = """
    SELECT dc.name AS city_name,
           ROUND(SUM(fss.BICYCLE_AVAILABLE) * 1.0 / SUM(ds.CAPACITTY), 3) AS avg_occupancy_rate
    FROM FACT_STATION_STATEMENT fss
    JOIN DIM_STATION ds ON fss.STATION_ID = ds.ID
    JOIN DIM_CITY dc ON fss.CITY_ID = dc.ID
    GROUP BY dc.name
    ORDER BY avg_occupancy_rate DESC;
"""
con.sql(requete).show()

# 4️⃣ Top 5 des stations les plus utilisées (par taux d’occupation moyen)
print("-- Top 5 des stations les plus utilisées (par taux d’occupation moyen)")
requete = """
    SELECT ds.name,
           ROUND(AVG(fss.BICYCLE_AVAILABLE * 1.0 / ds.CAPACITTY), 3) AS avg_occupancy_rate
    FROM FACT_STATION_STATEMENT fss
    JOIN DIM_STATION ds ON fss.STATION_ID = ds.ID
    GROUP BY ds.name
    ORDER BY avg_occupancy_rate DESC
    LIMIT 5;
"""
con.sql(requete).show()

# 5️⃣ Évolution du nombre total de vélos disponibles par jour


# 6️⃣ Station avec le plus grand nombre de vélos disponibles sur la dernière journée
print("-- Station avec le plus grand nombre de vélos disponibles sur la dernière journée")
requete = """
    SELECT ds.name, fss.BICYCLE_AVAILABLE
    FROM FACT_STATION_STATEMENT fss
    JOIN DIM_STATION ds ON fss.STATION_ID = ds.ID
    WHERE fss.CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM FACT_STATION_STATEMENT)
    ORDER BY fss.BICYCLE_AVAILABLE DESC
    LIMIT 1;
"""
con.sql(requete).show()

# 7️⃣ Répartition des stations par statut
print("-- Répartition des stations par statut")
requete = """
    SELECT STATUS, COUNT(*) AS nb_stations
    FROM DIM_STATION
    GROUP BY STATUS;
"""
con.sql(requete).show()

# 8️⃣ Capacité totale par ville
print("-- Capacité totale par ville")
requete = """
    SELECT dc.name AS city_name,
           SUM(ds.CAPACITTY) AS total_capacity
    FROM DIM_STATION ds
    JOIN FACT_STATION_STATEMENT fss ON ds.ID = fss.STATION_ID
    JOIN DIM_CITY dc ON fss.CITY_ID = dc.ID
    GROUP BY dc.name;
"""
con.sql(requete).show()

# 9️⃣ Taux de disponibilité moyen des bornes par ville
print("-- Taux de disponibilité moyen des bornes par ville")
requete = """
    SELECT dc.name AS city_name,
           ROUND(SUM(fss.BICYCLE_DOCKS_AVAILABLE) * 1.0 / SUM(ds.CAPACITTY), 3) AS avg_dock_availability_rate
    FROM FACT_STATION_STATEMENT fss
    JOIN DIM_STATION ds ON fss.STATION_ID = ds.ID
    JOIN DIM_CITY dc ON fss.CITY_ID = dc.ID
    GROUP BY dc.name;
"""
con.sql(requete).show()

# Fermeture de la connexion
con.close()
