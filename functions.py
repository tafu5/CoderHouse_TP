import pandas as pd
import requests
import psycopg2
import json
import zipfile
import io
import os
import datetime
from config import credentials
from airflow.models import Variable, XCom

def api_request(url):

    """ Devuelve los datos de la API en formato JSON si la conexión es exitosa 
        De lo contrario devuelve  False """

    response = requests.get(url)
    
    if response.status_code == 200:
        print('Conexión exitosa a la API')
        # Si el Request obtiene un JSON
        try:
            # Obtengo JSON
            raw_data = response.json()
            # Exporto el JSON a un archivo de texto
            with open('data/forecast/raw_data.txt', "w") as archivo:
                json.dump(raw_data, archivo)
        
        except:
            # Si el Request obtiene un archivo ZIP
            with zipfile.ZipFile(io.BytesIO(response.content)) as zip_:
                for txt in zip_.namelist():

                    # Obtengo el 'stop_times.txt'
                    if txt == 'stop_times.txt':
                        stop_times = pd.read_csv(zip_.open(txt), 
                                                 sep = ',')
                        # Exporto a CSV
                        stop_times.to_csv('data/static/stop_times.csv',
                                          index = False)

                    # Obtengo el 'trips.txt'
                    elif txt == 'trips.txt':
                        trips = pd.read_csv(zip_.open(txt), 
                                            sep = ',')
                        # Exporto a CSV
                        trips.to_csv('data/static/trips.csv',
                                     index = False)
                        
    else:
        print('No se pudo conectar a la API')
        raw_data = False
    

def trips_processing():
    """ Normaliza los datos del trips.csv """
    # Leo el CSV
    trips = pd.read_csv('data/static/trips.csv')
    # Reemplazo el CSV con los valores actuales
    trips[['trip_id', 'trip_headsign']].to_csv('data/static/trips_proc.csv',
                                               index = False)
    
    print('Datos normalizados con éxito!')
    
def stop_times_processing():
    """ Normaliza los datos del stop_times.csv """
    # Leo el CSV
    stop_times = pd.read_csv('data/static/stop_times.csv')
    # Paso columna 'arrival_time' a datetime
    stop_times['arrival_time'] = pd.to_datetime(stop_times['arrival_time'],
                                                 format = '%H:%M:%S')
    # Paso columna 'departure_time' a datetime
    stop_times['departure_time'] = pd.to_datetime(stop_times['departure_time'],
                                                   format = '%H:%M:%S')
    # Ordeno por 'trip_id' y 'stop_sequence'
    stop_times = stop_times.sort_values(by = ['trip_id', 'stop_sequence'],
                                        ascending = [True, True])
    # Calculo el tiempo en segundos entre cada estación 
    stop_times['trip_duration'] = stop_times.groupby('trip_id')['arrival_time'].\
                                  diff().dt.total_seconds()
    
    # Posiciono el 'trip_duration' en la fila correspondiente
    stop_times['trip_duration'] = stop_times.groupby('trip_id')['trip_duration'].\
                                  shift(-1).fillna(0).astype(int)
    
    # Reemplazo el CSV con los valores actuales
    selected_columns = ['trip_id', 'stop_id', 'trip_duration']
    stop_times[selected_columns].to_csv('data/static/stop_times_proc.csv',
                                        index = False)    
    
    print('Datos normalizados con éxito!')


def normalize_json(**kwargs):
    
    """ Devuelve el JSON normalizado en formato DataFrame y el timestamp del request.\
        Si el input es False devuelve (False, False) """
    
    with open("data/forecast/raw_data.txt", "r") as archivo:
        data_json = json.load(archivo)

    if data_json:
        # Normalizo el json
        df_norm = pd.json_normalize(data_json,
                                    record_path = ['Entity'],
                                    meta        = ['Header'] )
        
        # Creo columna 'timestamp' con el timestamp del request
        df_norm['timestamp'] = df_norm['Header'].apply(lambda x: x['timestamp'])

        # Elimino la columna Header del df_norm 
        df_norm.drop(columns = ['Header'],
                     inplace = True)
        
        # Nombre de la columna anidada
        nested_col_name = "Linea.Estaciones"

        # Columna anidada
        nested_column = df_norm[nested_col_name]
        # Desanido la columna que contiene valores anidados
        unnested_column = pd.concat([pd.json_normalize(row)
                                    for row in nested_column], 
                                    ignore_index=True) 
        
        # Elimino del df_norm la columna con valores anidados
        df_norm.drop(columns = [nested_col_name],
                     inplace = True)
        
        # Numero de veces que se debe repetir cada fila de df_norm
        repeats = [len(row) for row in nested_column]

        # Repito cada fila segun corresponda
        df_norm_expanded = pd.concat(
            [pd.DataFrame(row.repeat(rep).reshape(-1, rep).T,
                        columns = df_norm.columns) 
                        for row, rep in zip(df_norm.values, repeats)],
                        ignore_index = True) 
        
        assert len(df_norm_expanded) == len(unnested_column), "Los df deben tener el mismo numero de filas"
        
        # Concateno los dataframes 'df_norm_expanded' y 'unnested_column'
        df_final = pd.concat([df_norm_expanded, unnested_column],
                             axis = 1)
        
        # Renombro columnas
        df_final.columns = ['line_id', 'trip_id', 'route_id', 'direction_id', \
                            'start_time', 'start_date', 'timestamp_event', 'stop_id',\
                            'stop_name', 'arrival_time','arrival_delay',\
                            'departure_time', 'departure_delay']
        
        # Paso columnas a datetime
        for col in ['arrival_time', 'departure_time']:
            df_final[col] = df_final[col].apply(lambda x: datetime.datetime.fromtimestamp(x).time())

        df_final['start_date'] = pd.to_datetime(df_final['start_date'])
        
        # Creo columna time
        df_final['last_update_time'] = df_final['timestamp_event'].apply(lambda x: datetime.datetime.fromtimestamp(x).time())

        ## Union con datos estáticos
        # Importo datos estáticos
        stop_times = pd.read_csv('data/static/stop_times_proc.csv')
        trips = pd.read_csv('data/static/trips_proc.csv')
        
        # Agrego la columna 'trip_headsign' a 'df_final'
        df_final = pd.merge(left     = df_final,
                            right    = trips,
                            left_on  = 'trip_id',
                            right_on = 'trip_id')
        
        # Agrego la columna 'trip_duration' a 'df_final'
        df_final = pd.merge(left     = df_final, 
                            right    = stop_times,
                            left_on  = ['trip_id', 'stop_id'],
                            right_on = ['trip_id', 'stop_id'])

        # Borro columnas innecesarias
        for col in ['line_id', 'direction_id', 'departure_delay']:
            del df_final[col]

        # Cambio orden de columnas, asignando 'timestamp' como la primer columna
        df_final = df_final.loc[:, ['timestamp_event', 'last_update_time'] + [col for col in df_final if col not in ['timestamp_event', 'last_update_time']]]

        # Guardo el timestamp en una variable
        timestamp = df_final['timestamp_event'].unique()[0]

        # Elimino duplicados
        df_final = df_final.drop_duplicates()

        print('Datos normalizados con éxito!')
    else:
        print("El input 'data' debe ser tipo DataFrame")
        df_final, timestamp = False, False
    
    # Guardo los datos en la carpeta data/forecast
    df_final.to_csv('data/forecast/data_procesada.csv',
                    index = False)
    
    # Guardo el timestamp en xcom
    kwargs['ti'].xcom_push(key   = 'timestamp',
                           value = timestamp)


def database_connection():
    
    """ Devuelve la conexión a la base de datos si la misma es exitosa.\
        De lo contrario devuelve False """

    try:

        conn = psycopg2.connect(
            dbname   =  credentials['dbname'],
            user     =  credentials['user'],
            password =  credentials['password'],
            host     =  credentials['host'],
            port     =  credentials['port']
            )
        
        print("Conexión exitosa a la base de datos")

    except:
        conn = False
        print('No se ha podido conectar a la base de datos')
    
    return conn
    
def exist_table(table_name):

    """ Devuelve Booleano si la tabla se encuentra creada en Amazon RDS """
    # Conexión a la base de datos
    conn = database_connection()

    # Si existe una conexión a la base de datos
    if conn:

        # Creo cursor
        cursor = conn.cursor()
        query_table_exists = f""" 
                                SELECT table_name
                                FROM information_schema.tables
                                WHERE table_schema = '{credentials['user']}';
                                """
        
        # Obtengo los nombres de 
        cursor.execute(query_table_exists)
        tables_in_schema = cursor.fetchall()
        
        # Verifico si la tabla ya se encuentra en el Schema
        exist_table = any([table[0] == table_name for table in tables_in_schema])

        if exist_table:
            return 'forecast'
        else:
            return 'create_table_subte'
        

def exist_files():
     trips_path = '/opt/airflow/data/static/trips.csv'
     stop_times_path = '/opt/airflow/data/static/stop_times.csv'

     exist = os.path.exists(trips_path) and os.path.exists(stop_times_path)

     if exist:
         return 'forecast'
     else:
         return 'static_api_request'
     
def timestamp_check(**kwargs):
    # Me conecto a la Base de Datos RDS
    conn = database_connection()

    # Obtengo el timestamp obtenido del request a la API almacenado en Xcom
    timestamp = kwargs['ti'].xcom_pull(task_ids = 'transform', 
                                       key = 'timestamp')
    
    # Creo cursor 
    cursor = conn.cursor()

    # Obtengo los eventos ya cargados en la tabla
    cursor.execute(f"""
                        SELECT DISTINCT(timestamp_event) FROM subte  
                        """)
        
    event_check = cursor.fetchall()

    if any([timestamp == date[0] for date in event_check]):
        return 'already_stored'
    else:
        return 'load'

def data_ingestion(**kwargs):
    
    """ Ingesta datos normalizados a una tabla de la base de datos """
    # Me conecto a la Base de Datos RDS
    conn = database_connection()
    
    # Importo datos procesados
    data = pd.read_csv('data/forecast/data_procesada.csv',
                       sep =',')
    
    
    if data is not False and conn is not False:
        
        # Creo cursor 
        cursor = conn.cursor()

        
        # Variables para crear la inserción a la base de datos
        columns = ", ".join(data.columns)
        s = ", ".join( ['%s'] * len(data.columns) )

        # Inserción de valores SQL
        sql = f"""
                INSERT INTO subte ({columns})
                VALUES ({s}) """

        # Valores a ingestar la base de datos
        values = [tuple(row) for row in data.values]
            
        try:
            # Bucle de inserción por filas
            for val in values:
                cursor.execute(sql, val)
                conn.commit()

            print(f"Se ingresaron {len(data)} registros en la tabla 'subte'")
        except Exception as exeption:
            print(exeption)
            print('Hubo un error al momento de ingresar los datos a la base de datos')
        
        cursor.close()
    
    elif data:
        print("Debe contecarse a una base de datos para ingestar valores a la tabla")
    
    else:
        print("El input 'data' debe ser tipo DataFrame")


def email_check(**kwargs):
    
    alert_count = Variable.get('alert_count', 
                               default_var=0)
    

    data = pd.read_csv('data/forecast/data_procesada.csv',
                       sep =',')
    
    data['start_date'] = pd.to_datetime(data['start_date'])
    data['day'] = data['start_date'].apply(lambda x: x.strftime("%A"))
    data['day'] = data['day'].apply(lambda x: 'Monday_Friday' if x not in ['Saturday', 'Sunday'] else x)
    data['trip'] = data['trip_id'].apply(lambda x:x[0])
    
    delays = pd.read_csv('data/forecast/delays.csv')


    data = data.merge(delays,
                      how = 'left',
                      on=['trip', 'day'])
    
    data_filt = data[data['arrival_delay'] > data['frequency']].reset_index()

    if (len(data_filt)>0) & (int(alert_count) < 2):

        body_mail = []
        for i in range(len(data_filt)):
            body_mail.append(f"""
                            - La estación {data_filt.loc[i, 'stop_name']} de la {data_filt.loc[i, 'route_id']} con dircción {data_filt.loc[i, 'trip_headsign']} tiene una demora de {data_filt.loc[i, 'arrival_delay']} segundos""")
        
        kwargs['ti'].xcom_push(key = 'alert_message', 
                            value = "\n".join(body_mail))
        
         # Increment the alert count
        Variable.set('alert_count', int(alert_count) + 1)

        return 'send_email'
    return 'end_dag'

