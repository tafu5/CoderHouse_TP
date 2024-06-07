## Data Engineer

Alumno: Valentín Tafura

El proyecto consta de extraer datos de la API TRANSPORTE DE BUENOS AIRES, crear un proceso ETL y cargarlos a una tabla de Amazon Redshift

- API link: https://api-transporte.buenosaires.gob.ar/console
<<<<<<< Updated upstream
- Granuralidad: La API provee información GTFS dinámica y estática subtes dentro de la CABA con una granularidad medida en minutos (1 minuto aprox.)

Aclaración: Para poder correr el DAG es necesario crear una conexión a Amazon RDS mediante la UI de Airflow y llamarla 'my_rds_connection"

### Screenshots

- UI de Airflow con el DAG
<img width="1424" alt="foto1" src="https://github.com/tafu5/CoderHouse_TP/assets/55017296/21e823f7-ea61-4d50-833c-8b5356ebb85d">

- Tareas
<img width="1397" alt="Screenshot 2024-06-07 at 2 43 09 PM" src="https://github.com/tafu5/CoderHouse_TP/assets/55017296/e78bad76-44da-4fa1-9aa8-8582134d1399">

#### Static files

Son los datos estáticos obtenidos desde la API. De ellos se obtendrán el nombre estación terminal de la linea y el tiempo que se demora entre cada estación (medido en segundos)
- exist_static_files (BranchPythonOperator): Verifica si ya existen los archivos estáticos (stop_train.csv y trip.csv). Si existen sigue hacia 'forecast' y si no existen se hace el request a la API.
- static_api_request (PythonOperator): Obtiene los datos estáticos desde la API y se guardan en la carpeta data/static
- static_processing (PythonOperator): Procesa los datos estáticos y se guardan en la carpeta data/static

#### table_rds
- exist_table_subte (BranchPythonOperator): Verifica si existe una tabla llamada 'subte' en Amazon RDS. Si ya existe sigue hacia la tarea 'forecast' y sino la crea.
- create_table_subte (PostgresOperator): Crea una tabla llamada 'subte' en Amazon RDS

#### Forecast
- sensor_files (TaskGroup): Sensa que los archivos estáticos esten almacenados en la carpeta correspondiente. Incluye un SensorFile para cada uno de ellos 
- extract (PythonOperator): Extrae los datos dinámicos de la API y los almacena en data/forecast 
- transform: Transforma los datos dinámicos de la API y los almacena en data/forecast  
- timestamp_check (BranchPythonOperator): Verifica si el timestamp extraido al momento del request a la API ya se encuentra en la tabla 'subte' de Amazon RDS. Si ya se encuentra sigue hacia la tarea 'already_stored', de lo contrario sigue hacia 'load'.
- already_stored (BashOperator): Imprime que el timestamp ya se encuentra en la base de datos
- load (PythonOperator): Carga los datos dinámicos procesados a la tabla 'subte' en RDS.
- alert (PythonOperator): Verifíca si hay que enviar una alerta por mail en caso que algun servicio este demorado mas de lo planificado (esta información se encuentra en el delays.csv)
- send_email (EmailOperator): Envía email en caso de haberse activado a alarma
- end_dag (BashOperator): Imprime que no se envía alerta por no haber demoras en los servicios o porque se alcanzó el máximo de 2 envíos de emails.


- Status de las tareas
<img width="249" alt="Screenshot 2024-06-07 at 2 42 46 PM" src="https://github.com/tafu5/CoderHouse_TP/assets/55017296/7b6feeb5-5c70-48ec-adb2-12d2bb080faa">

- Como se puede visualizar, en la primer instancia del DAG se crea la tabla 'subte' en Amazon RDS y se crean los archivos estáticos. Estas tareas solo se realizan una vez y luego el flujo las saltea para no duplicar las tareas.
- En la tercera instancia el timestamp ya se encontraba creado en la tabla 'subte' en Amazon RDS y por lo tanto se saltea las etapas siguientes.

