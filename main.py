from functions import  exist_table, exist_files,api_request, trips_processing, stop_times_processing, normalize_json, timestamp_check, data_ingestion, email_check
from config import client_id, client_secret
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.task_group import TaskGroup
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email_operator import EmailOperator
from datetime import datetime, timedelta

url_static = f"https://apitransporte.buenosaires.gob.ar/subtes/feed-gtfs?client_id={client_id}&client_secret={client_secret}"

sql_table_string = f""" 
                CREATE TABLE IF NOT EXISTS subte (
                    timestamp_event INT PRIMARY KEY,
                    last_update_time TIME,
                    trip_id VARCHAR(3),
                    route_id VARCHAR(6),
                    start_time TIME,
                    start_date DATE,
                    stop_id VARCHAR(5),
                    stop_name VARCHAR(26),
                    arrival_time TIME,    
                    arrival_delay SMALLINT,
                    departure_time TIME,
                    trip_headsign VARCHAR(100),
                    trip_duration SMALLINT
                    );
            """

# Argumentos para el DAG
default_args = {
    'owner': 'ValentinTafura',
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
    'email': 'valentintafura@hotmail.com',
    'email_on_retry': True,
    'email_on_failure': True,
    'catchup': False
}

with DAG(
    default_args=default_args,
    dag_id='ETL',
    description='DAG para correr un ETL diariamente desde una API a una base de datos en Amazon RDS',
    start_date=datetime(2024, 6, 5),
    schedule_interval=timedelta(minutes=1),
    catchup=False,
    max_active_runs = 4, # Permitir hasta 4 instancias simultáneas del DAG
    concurrency = 4 # Permitir hasta 4 tareas en paralelo
) as dag:
    
    table_rds = DummyOperator(
        task_id='table_rds'        
    )

    # Operador de branching
    exist_table_subte = BranchPythonOperator(
        task_id = 'exist_table_subte',
        python_callable = exist_table,
        op_kwargs = {'table_name': 'subte'},
        provide_context = True
        )

    create_table = PostgresOperator(
        task_id = 'create_table_subte',
        postgres_conn_id = 'my_rds_connection',
        sql = sql_table_string
        )
    
    static_files = DummyOperator(
        task_id ='static_files',
        trigger_rule='all_done'
    )
    
    # Operador de branching
    exist_static_files = BranchPythonOperator(
        task_id = 'exist_static_files',
        python_callable = exist_files,
        provide_context = True
        )
    
    forecast = DummyOperator(
        task_id='forecast',
        trigger_rule = 'all_done'
    )
    
    with TaskGroup('sensor_files') as sensor_files:

        stop_times_sensor_file = FileSensor(
            task_id="stop_times.csv_exists",
            poke_interval=10,
            timeout=60 * 30,
            filepath='/opt/airflow/data/static/stop_times.csv'
            )
        
        trip_sensor_file = FileSensor(
            task_id="trips.csv_exists",
            poke_interval=10,
            timeout=60 * 30,
            filepath='/opt/airflow/data/static/trips.csv'
            )
        
    static_request = PythonOperator(
        task_id         = 'static_api_request',
        python_callable =  api_request,
        op_kwargs = {'url': f"https://apitransporte.buenosaires.gob.ar/subtes/feed-gtfs?client_id={client_id}&client_secret={client_secret}"},
        provide_context = True
    )

    with TaskGroup('static_processing') as static_processing:

        normalize_trips = PythonOperator(
            task_id         = 'trips_processing',
            python_callable = trips_processing,
            provide_context = True
        )

        normalize_stop_times = PythonOperator(
            task_id         = 'stop_times_processing',
            python_callable = stop_times_processing,
            provide_context = True
        )

    forecast_request = PythonOperator(
        task_id         = 'extract',
        python_callable = api_request,
        op_kwargs= {'url': f"https://apitransporte.buenosaires.gob.ar/subtes/forecastGTFS?client_id={client_id}&client_secret={client_secret}"},
        provide_context = True
    )

    transform = PythonOperator(
        task_id         = 'transform',
        python_callable = normalize_json,
        provide_context = True
    )

    # Operador de branching
    timestamp_check = BranchPythonOperator(
        task_id = 'timestamp_check',
        python_callable = timestamp_check,
        provide_context = True
        )
    
    already_stored =BashOperator(
        task_id = 'already_stored',
        bash_command='echo "El evento ya se encontraba guardado en la tabla"'
    )

    load = PythonOperator(
        task_id         = 'load',
        python_callable = data_ingestion,
        provide_context = True
    )

    alert = PythonOperator(
        task_id         = 'alert',
        python_callable = email_check,
        provide_context = True
    )

    send_email = EmailOperator(
        task_id='send_email',
        to='valentintafura@hotmail.com',
        subject='Demoras en el servicio',
        html_content="""{{ task_instance.xcom_pull(task_ids='alert', key='alert_message') }}""",
    )

    end_dag = PythonOperator(
        task_id='end_dag',
        python_callable=lambda: print("No hay alertas o se alcanzo el limite máximo de 2 mails."),
    )


    email_check

## Dependencias
"""
# Seccion tabla RDS: 
table_rds >> exist_table_subte >> [create_table, static_files]
create_table >> static_files

# Seccion datos estaticos
static_files >> exist_static_files
exist_static_files >> [forecast, static_request]
static_request >> static_processing
static_processing >> forecast

# Seccion obtencion datos forecast
forecast >> sensor_files >> forecast_request >> transform >> timestamp_already_stored
timestamp_already_stored >> [print, load]
"""

# Seccion tabla RDS: 
table_rds >> exist_table_subte >> [create_table, forecast]
create_table >> forecast

# Seccion datos estaticos
static_files >> exist_static_files
exist_static_files >> [forecast, static_request]
static_request >> static_processing
static_processing >> forecast

# Seccion obtencion datos forecast
forecast >> sensor_files >> forecast_request >> transform >> timestamp_check
timestamp_check >> [already_stored, load]
load >> alert
alert >> send_email
alert >> end_dag