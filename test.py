# ./duckdb data/duckdb/mobility_analysis.duckdb
# .exit
import duckdb               

con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = True)
print("-- Nb d'emplacements disponibles de vélos dans une ville")
requete = """
          SELECT dm.NAME, tmp.SUM_BICYCLE_DOCKS_AVAILABLE 
          FROM DIM_CITY dm INNER JOIN (
            SELECT CITY_ID, SUM(BICYCLE_DOCKS_AVAILABLE) AS SUM_BICYCLE_DOCKS_AVAILABLE 
            FROM FACT_STATION_STATEMENT WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) 
            FROM CONSOLIDATE_STATION) 
          GROUP BY CITY_ID) tmp ON dm.ID = tmp.CITY_ID WHERE lower(dm.NAME) in ('paris', 'nantes', 'vincennes', 'toulouse');
          """
con.sql(requete).show()
print("-- Nb de vélos disponibles en moyenne dans chaque station")
requete = """
          SELECT ds.name, ds.code, ds.address, tmp.avg_dock_available
          FROM DIM_STATION ds JOIN ( 
            SELECT station_id, AVG(BICYCLE_AVAILABLE) AS avg_dock_available 
            FROM FACT_STATION_STATEMENT GROUP BY station_id ) 
          AS tmp ON ds.id = tmp.station_id;
          """
con.sql(requete).show()
requete = """
          SELECT * from CONSOLIDATE_STATION where city_name ='nantes';
          """
con.sql(requete).show()
requete = """
          SELECT * from CONSOLIDATE_STATION_STATEMENT where STATION_ID like '2%';
          """
con.sql(requete).show()
con.close()