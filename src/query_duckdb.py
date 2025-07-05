import duckdb

# Connexion à la base DuckDB
con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=True)

# 📊 1️⃣ Nombre d'emplacements disponibles pour les vélos dans une ville
print("-- Nombre d'emplacements disponibles pour les vélos dans une ville")
requete = """
    SELECT dm.NAME, tmp.SUM_BICYCLE_DOCKS_AVAILABLE
    FROM DIM_CITY dm
    INNER JOIN (
        SELECT CITY_ID, SUM(BICYCLE_DOCKS_AVAILABLE) AS SUM_BICYCLE_DOCKS_AVAILABLE
        FROM FACT_STATION_STATEMENT
        WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION)
        GROUP BY CITY_ID
    ) tmp ON dm.ID = tmp.CITY_ID
    WHERE lower(dm.NAME) IN ('paris', 'nantes', 'strasbourg', 'toulouse');
"""
con.sql(requete).show()

# 📊 2️⃣ Moyenne des vélos disponibles par station
print("-- Moyenne des vélos disponibles par station")
requete = """
    SELECT ds.NAME, ds.CODE, ds.ADDRESS, tmp.AVG_DOCK_AVAILABLE
    FROM DIM_STATION ds
    JOIN (
        SELECT STATION_ID, AVG(BICYCLE_AVAILABLE) AS AVG_DOCK_AVAILABLE
        FROM FACT_STATION_STATEMENT
        GROUP BY STATION_ID
    ) tmp ON ds.ID = tmp.STATION_ID;
"""
con.sql(requete).show()

# 📊 3️⃣ Capacité totale par ville
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

# Fermeture de la connexion
con.close()
