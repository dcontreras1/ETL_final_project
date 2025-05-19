from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
import pandas as pd
from sqlalchemy import create_engine
import logging
import time

def stream_unemployment():
    with open('/home/dcontreras/ETL_final_project/credentials.json') as f:
        creds = json.load(f)

    engine = create_engine(
        f"postgresql://{creds['user']}:{creds['password']}@{creds['host']}:{creds['port']}/{creds['database']}"
    )

    try:
        # Intentar conectar con el broker de Kafka
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    except NoBrokersAvailable as e:
        logging.error("No se pudo conectar a Kafka: Broker no disponible.")
        raise RuntimeError("Kafka no está disponible. Verifica que el broker esté corriendo.") from e

    query = """
        SELECT c.country_name, t.year, f.unemployment
        FROM fact_economy f
        JOIN dim_country c ON f.country_id = c.country_id
        JOIN dim_time t ON f.year = t.year
        WHERE f.unemployment IS NOT NULL
    """

    df = pd.read_sql(query, engine)

    if not df.empty:
        for _, row in df.iterrows():
            metric = {
                'country': row['country_name'],
                'indicator': 'unemployment',
                'year': int(row['year']),
                'value': row['unemployment']
            }
            try:
                # Enviar a Kafka una fila a la vez
                producer.send('unemployment_topic', metric)
                logging.info(f"Enviado a Kafka desde Airflow: {metric}")
                # Cooldown de 1 segundo entre cada fila
                time.sleep(1)
            except Exception as e:
                logging.error(f"Error al enviar a Kafka: {e}")
                raise
    else:
        logging.info("No hay métricas válidas para streamear.")
