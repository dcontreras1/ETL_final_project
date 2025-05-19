import logging
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

def load():
    logging.info("Starting data loading from economy_merged")

    try:
        # Conectarse a PostgreSQL con el hook
        postgres_hook = PostgresHook(postgres_conn_id="PSQL_ETL_project_conn")
        engine = postgres_hook.get_sqlalchemy_engine()

        # Leer la tabla final desde la base de datos
        df = pd.read_sql("SELECT * FROM economy_merged", con=engine)

        # Renombrar correctamente la columna country_id_x a country_id (si existe)
        if "country_id_x" in df.columns:
            df = df.rename(columns={"country_id_x": "country_id"})

        # Tablas dimensionales y de hechos
        dim_country = df[['country_id', 'country_name', 'currency']].drop_duplicates()
        dim_time = df[['year']].drop_duplicates()

        fact_economy = df[[
            'country_id', 'year', 'gdp', 'gni', 'population',
            'per_capita_gni', 'imports_of_goods_and_services', 'household_expenditure',
            'gdp_per_capita', 'imports_per_capita', 'household_expenditure_per_capita',
            'inflation', 'unemployment'  # columnas agregadas desde API
        ]]

        # Cargar tablas dimensionales y de hechos
        dim_country.to_sql('dim_country', con=engine, if_exists='replace', index=False)
        dim_time.to_sql('dim_time', con=engine, if_exists='replace', index=False)
        fact_economy.to_sql('fact_economy', con=engine, if_exists='replace', index=False)

        logging.info("Data successfully loaded into dimensional model")

    except Exception as e:
        logging.error(f"Loading error: {e}")
        raise
