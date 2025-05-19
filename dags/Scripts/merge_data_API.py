import pandas as pd
from sqlalchemy import create_engine
import json
import logging

def merge():
    logging.info("Iniciando el proceso de merge entre economy_transformed y api_data")

    try:
        # Leer credenciales desde el archivo JSON
        with open("credentials.json") as f:
            config = json.load(f)

        user = config["user"]
        password = config["password"]
        host = config["host"]
        port = config["port"]
        database = config["database"]

        # Crear engine SQLAlchemy
        engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")

        # Leer tablas desde PostgreSQL
        df_economy = pd.read_sql("SELECT * FROM economy_transformed", engine)
        df_api = pd.read_sql("SELECT * FROM api_data", engine)

        # Limpiar espacios en los nombres de país
        df_economy['country_name'] = df_economy['country_name'].str.strip()
        df_api['country_name'] = df_api['country_name'].str.strip()

        # Realizar merge por country_name y year
        df_merged = pd.merge(df_economy, df_api, on=["country_name", "year"], how="left")

        # Eliminar columna innecesaria si existe (por ejemplo, country_id_y)
        if 'country_id_y' in df_merged.columns:
            df_merged.drop(columns=['country_id_y'], inplace=True)

        # Guardar resultado en nueva tabla
        df_merged.to_sql("economy_merged", engine, if_exists="replace", index=False)
        logging.info("Merge exitoso. Datos guardados en la tabla 'economy_merged'.")

    except Exception as e:
        logging.error(f"Error durante el merge: {e}")
        raise
