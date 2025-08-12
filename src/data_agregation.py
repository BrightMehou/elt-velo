from duckdb_tools import exec_sql_from_file

def data_agregation() -> None:
    exec_sql_from_file('alim_agregate_tables.sql', 'Agrégation des tables.')
