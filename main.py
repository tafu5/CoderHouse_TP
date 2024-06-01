from functions import create_table, request_api, normalize_json, data_ingestion
from config import credentials
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Creacion de la tabla
create_table(table_name = 'subte', 
             credentials = credentials)

# Argumentos para el DAG
default_args = {
    'owner': 'ValentinTafura',
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
    'catchup' : True
}

with DAG(
    default_args = default_args,
    dag_id = 'ETL',
    description = 'DAG para correr un ETL diariamente desde una API a una base de datos en Amazon RDS',
    start_date = datetime(2024, 5, 30),
    schedule_interval = '@daily',
    catchup = True
    ) as dag:

    task_1 = PythonOperator(
        task_id         = 'extract_task',
        python_callable = request_api,
        provide_context = True
    )

    task_2 = PythonOperator(
        task_id         = 'transform_task',
        python_callable = normalize_json,
        provide_context = True
    )

    task_3 = PythonOperator(
        task_id         = 'load_task',
        python_callable = data_ingestion,
        provide_context = True
    )

task_1 >> task_2 >> task_3