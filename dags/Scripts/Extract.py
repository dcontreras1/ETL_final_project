import pandas as pd
import logging
import os
from airflow.providers.postgres.hooks.postgres import PostgresHook

def extract():
    logging.info("Starting data extraction from database")

    output_path = "/home/dcontreras/ETL/dags/temp/extracted.csv"

    # Verificar si el archivo ya existe
    if os.path.exists(output_path):
        logging.info(f"File already exists at {output_path}, skipping extraction.")
        return

    try:
        # Conexión a PostgreSQL
        postgres_hook = PostgresHook(postgres_conn_id="PSQL_ETL_project_conn")
        engine = postgres_hook.get_sqlalchemy_engine()

        # Leer datos desde la tabla cruda
        df = pd.read_sql("SELECT * FROM economy_raw", engine)

        # Guardar a CSV
        df.to_csv(output_path, index=False)
        logging.info(f"Data extracted and saved to {output_path}")

    except Exception as e:
        logging.error(f"Extraction error: {e}")
        raise
