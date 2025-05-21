from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from Scripts.Extract import extract
from Scripts.Extract_API import fetch_api_data
from Scripts.Transform import transform
from Scripts.merge_data_API import merge
from Scripts.Load import load
from Scripts.Producer_kafka import stream_unemployment
from Scripts.validate_data import validate_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 1),
    'email': ['d.contreras_d@uao.edu.co'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'ETL_economy_pipeline',
    default_args=default_args,
    description='ETL DAG for Global Economy Indicators',
    schedule_interval='@daily',
    catchup=False,
    tags=['ETL', 'economy', 'kafka']
)

extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag
)

fetch_api_task = PythonOperator(
    task_id='fetch_api_data',
    python_callable=fetch_api_data,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform,
    dag=dag
)

merge_task = PythonOperator(
    task_id='merge',
    python_callable=merge,
    dag=dag
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load,
    dag=dag
)

stream_task = PythonOperator(
    task_id='stream',
    python_callable=stream_unemployment,
    dag=dag
)

validate_task = PythonOperator(
    task_id='validate',
    python_callable=validate_data,
    dag=dag
)

# Dependencias: extract y fetch_api_data en paralelo
[extract_task, fetch_api_task] >> transform_task >> merge_task >> load_task >> stream_task >> validate_task
