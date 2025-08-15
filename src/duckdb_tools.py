import logging
import os

import duckdb

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

duckdb_path = "data/duckdb/mobility_analysis.duckdb"

s3_endpoint = os.getenv("minio_endpoint", "localhost:9000")
s3_access_key = os.getenv("minio_access_key", "minioadmin")
s3_secret_key = os.getenv("minio_secret_key", "miniopassword")

s3_statement = f"""
INSTALL httpfs;
LOAD httpfs;
SET s3_url_style='path';
SET s3_use_ssl='false';
SET s3_endpoint='{s3_endpoint}';
SET s3_access_key_id='{s3_access_key}';
SET s3_secret_access_key='{s3_secret_key}';
SET s3_region='us-east-1';
"""


def exec_sql_from_file(
    file_name: str, log_message: str, connect_to_minio: bool = False
) -> None:
    """
    Crée les tables définies dans un fichier SQL.

    Les instructions SQL sont lues depuis le fichier spécifié,
    situé dans le répertoire `src/sql_statements`, et exécutées
    une par une sur la base de données `mobility_analysis.duckdb`.
    """
    con = duckdb.connect(database=duckdb_path, read_only=False)
    if connect_to_minio:
        con.execute(s3_statement)
    with open(f"src/sql_statements/{file_name}") as fd:
        statements = fd.read()

        for statement in statements.split(";"):
            con.execute(statement)
        logger.info(log_message)


def exec_sql_statments(sql_statments: str, connect_to_minio: bool = False) -> None:
    """
    Exécute les instructions SQL fournies.

    Cette fonction est utilisée pour exécuter des instructions SQL
    directement dans la base de données `mobility_analysis.duckdb`.
    """
    con = duckdb.connect(database=duckdb_path, read_only=False)
    if s3_statement:
        con.execute(s3_statement)
    for statement in sql_statments.split(";"):
        con.execute(statement)


def get_city_code(name: str) -> str:
    """
    Récupère le code d'une ville depuis la table `CONSOLIDATE_CITY` en fonction de son nom.

    Args:
        name (str): Nom de la ville (non sensible à la casse).

    Returns:
        str: Code de la ville correspondante.

    Note:
        - Le code est extrait des données les plus récentes (basées sur la colonne `CREATED_DATE`).
    """

    con = duckdb.connect(database=duckdb_path, read_only=False)

    requete = f"""SELECT ID FROM CONSOLIDATE_CITY 
             WHERE lower(NAME) = '{name}'
             AND CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_CITY)"""
    code = con.sql(requete).fetchall()[0][0]

    return code
