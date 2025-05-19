import requests
import pandas as pd
from sqlalchemy import create_engine
import json

with open('/home/dcontreras/ETL_final_project/credentials.json') as f:
    creds = json.load(f)

engine = create_engine(f"postgresql://{creds['user']}:{creds['password']}@{creds['host']}:{creds['port']}/{creds['database']}")

# Indicadores: clave = código API, valor = nombre amigable para columna
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
        if len(data) < 2:
            break
        page_data = data[1]
        if not page_data:
            break
        records.extend(page_data)
        if page >= int(data[0]['pages']):
            break
        page += 1
    return records

# DataFrame vacío para juntar todos los indicadores
df_all = pd.DataFrame()

# Recorrer indicadores
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

    # Fusionar horizontalmente
    if df_all.empty:
        df_all = df
    else:
        df_all = pd.merge(df_all, df, on=["country_id", "country_name", "year"], how="outer")

# Ordenar
df_all = df_all.sort_values(by=["country_id", "year"])

# Guardar en PostgreSQL
df_all.to_sql("api_data", engine, if_exists="replace", index=False)
print("Datos guardados en la tabla api_data.")
