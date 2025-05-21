import requests
import pandas as pd
from sqlalchemy import create_engine, inspect
import json

def fetch_api_data():
    with open('/home/dcontreras/ETL_final_project/credentials.json') as f:
        creds = json.load(f)

    engine = create_engine(
        f"postgresql://{creds['user']}:{creds['password']}@{creds['host']}:{creds['port']}/{creds['database']}"
    )

    inspector = inspect(engine)
    if "api_data" in inspector.get_table_names():
        print("La tabla 'api_data' ya existe. No se sobreescribirá.")
        return

    # Indicadores de la API del World Bank
    indicators = {
        "FP.CPI.TOTL.ZG": "inflation",
        "SL.UEM.TOTL.ZS": "unemployment"
    }

    def fetch_all_pages(base_url):
        records = []
        page = 1
        while True:
            url = f"{base_url}&page={page}"
            response = requests.get(url)
            if response.status_code != 200:
                break
            data = response.json()
            if len(data) < 2 or not data[1]:
                break
            records.extend(data[1])
            if page >= int(data[0]['pages']):
                break
            page += 1
        return records

    # Acumulador de resultados
    df_all = pd.DataFrame()

    for indicator_code, column_name in indicators.items():
        print(f"Extrayendo datos de {column_name}...")

        url = f"http://api.worldbank.org/v2/country/all/indicator/{indicator_code}?format=json&date=2000:2023&per_page=500"
        data = fetch_all_pages(url)

        df = pd.DataFrame(data)
        df = df[["country", "date", "value"]].dropna()
        df["country_id"] = df["country"].apply(lambda x: x['id'])
        df["country_name"] = df["country"].apply(lambda x: x['value'])
        df["year"] = df["date"].astype(int)
        df[column_name] = df["value"]
        df = df[["country_id", "country_name", "year", column_name]]

        if df_all.empty:
            df_all = df
        else:
            df_all = pd.merge(df_all, df, on=["country_id", "country_name", "year"], how="outer")

    df_all = df_all.sort_values(by=["country_id", "year"])
    df_all.to_sql("api_data", engine, if_exists="replace", index=False)
    print("Datos guardados en la tabla 'api_data'.")
