from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

def transform():
    logging.info("Starting transformation in PostgreSQL")

    try:
        hook = PostgresHook(postgres_conn_id="PSQL_ETL_project_conn")

        sql = """
        DROP TABLE IF EXISTS economy_transformed;

        CREATE TABLE economy_transformed AS
        SELECT
            " CountryID " AS country_id,
            " Country " AS country_name,
            " Year " AS year,
            " AMA exchange rate " AS ama_exchange_rate,
            " IMF based exchange rate " AS imf_exchange_rate,
            " Population " AS population,
            " Currency " AS currency,
            " Per capita GNI " AS per_capita_gni,
            " Gross National Income(GNI) in USD " AS gni,
            " Gross Domestic Product (GDP) " AS gdp,
            " Imports of goods and services " AS imports_of_goods_and_services,
            " Household consumption expenditure (including Non-profit instit" AS household_expenditure,

           -- Indicadores derivados
CASE WHEN " Population " > 0 THEN " Gross Domestic Product (GDP) " / " Population " ELSE NULL END AS gdp_per_capita,
CASE WHEN " Population " > 0 THEN " Imports of goods and services " / " Population " ELSE NULL END AS imports_per_capita,
CASE WHEN " Population " > 0 THEN " Household consumption expenditure (including Non-profit instit " / " Population " ELSE NULL END AS household_expenditure_per_capita

        FROM economy_raw;
        """

        hook.run(sql)
        logging.info("Transformation complete and economy_transformed table created.")

    except Exception as e:
        logging.error(f"Transformation failed: {e}")
        raise
