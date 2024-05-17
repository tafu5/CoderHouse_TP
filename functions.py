import pandas as pd
import requests
import psycopg2


def request_api(client_id: str, client_secret: str):

    """ Devuelve los datos de la API en formato JSON si la conexión es exitosa 
        De lo contrario devuelve  False """

    url = f"https://apitransporte.buenosaires.gob.ar/subtes/forecastGTFS?client_id={client_id}&client_secret={client_secret}"
    response = requests.get(url)
    
    if response.status_code == 200:
        print('Conexión exitosa a la API')
        data = response.json()

    else:
        print('No se pudo conectar a la API')
        data = False
    
    return data


def normalize_json(data_json: dict) -> pd.DataFrame:
    """ Devuelve el JSON normalizado en formato DataFrame y el timestamp del request.\
        Si el input es False devuelve (False, False) """
    if data_json:
        # Normalizo el json
        df_norm = pd.json_normalize( data_json,
                                        record_path = ['Entity'],
                                        meta = ['Header'] )
        
        # Creo columna 'timestamp' con el timestamp del request
        df_norm['timestamp'] = df_norm['Header'].apply(lambda x: x['timestamp'])

        
        # Elimino la columna Header del df_norm 
        df_norm.drop(columns=['Header'],
                    inplace=True)
        
        # Nombre de la columna anidada
        nested_col_name = "Linea.Estaciones"

        # Columna anidada
        nested_column = df_norm[nested_col_name]
        # Desanido la columna que contiene valores anidados
        unnested_column = pd.concat([pd.json_normalize(row)
                                    for row in nested_column], 
                                    ignore_index=True) 
        
        # Elimino del df_norm la columna con valores anidados
        df_norm.drop(columns=[nested_col_name],
                    inplace=True)
        
        # Numero de veces que se debe repetir cada fila de df_norm
        repeats = [len(row) for row in nested_column]

        # Repito cada fila segun corresponda
        df_norm_expanded = pd.concat(
            [pd.DataFrame(row.repeat(rep).reshape(-1, rep).T,
                        columns=df_norm.columns) 
                        for row, rep in zip(df_norm.values, repeats)],
                        ignore_index=True) 
        
        assert len(df_norm_expanded) == len(unnested_column), "Los df deben tener el mismo numero de filas"
        
        # Concateno los dataframes 'df_norm_expanded' y 'unnested_column'
        df_final = pd.concat([df_norm_expanded, unnested_column],
                        axis=1)
        
        # Renombro columnas
        df_final.columns = ['line_id', 'trip_id', 'route_id', 'direction_id', \
                            'start_time', 'start_date', 'timestamp_event', 'stop_id',\
                            'stop_name', 'arrival_time','arrival_delay',\
                            'departure_time', 'departure_delay']
        
        # Cambio orden de columnas, asignando 'timestamp' como la primer columna
        df_final = df_final.loc[:, ['timestamp_event'] + [col for col in df_final if col != 'timestamp_event']]

        
        # Paso a formato datetime las columnas relacionadas con fechas
        df_final['timestamp_event'] = pd.to_datetime(df_final['timestamp_event'].astype(int), unit='s')
        df_final['arrival_time']    = pd.to_datetime(df_final['arrival_time'], unit='s')
        df_final['departure_time']  = pd.to_datetime(df_final['departure_time'], unit='s')
        df_final['start_date']      = pd.to_datetime(df_final['start_date'])

        # Elimino duplicados
        df_final = df_final.drop_duplicates()
        
        # Guardo el timestamp en una variable
        timestamp = df_final['timestamp_event'].unique()[0]  

        print('Datos normalizados con éxito')
    else:
        print("El input 'data' debe ser tipo DataFrame")
        df_final, timestamp = False, False
        
    return df_final, timestamp
    

def database_connection(credentials: dict):
    
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
    


def create_table(conn, table_name: str, credentials: dict):
    
    """ Crea una tabla en la base de datos si existe una conexión a la base de datos """

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
        existe_table = any([table[0] == table_name for table in tables_in_schema])
        
        # Si la tabla ya existe
        if existe_table:

            print(f"La tabla '{table_name}' ya se encuentra creada")

        # Si la tabla no existe, se crea
        else:
            query = f""" 
                CREATE TABLE IF NOT EXISTS {table_name} (
                    timestamp_event TIMESTAMP PRIMARY KEY,
                    line_id VARCHAR(10),
                    trip_id VARCHAR(3),
                    route_id VARCHAR(6),
                    direction_id SMALLINT,
                    start_time TIME,
                    start_date TIMESTAMP,
                    stop_id VARCHAR(5),
                    stop_name VARCHAR(26),
                    arrival_time TIMESTAMP,	
                    arrival_delay SMALLINT,
                    departure_time TIMESTAMP,
                    departure_delay SMALLINT
                    );
            """

            cursor.execute(query)
            conn.commit()
            print(f"Tabla '{table_name}' creada")
            cursor.close()
        
    else:
        print("Debe contecarse a una base de datos para crear una tabla")


def data_ingestion(conn, data: pd.DataFrame, timestamp, table_name: str):
    
    """ Ingesta datos normalizados a una tabla de la base de datos """

    if data is not False and conn is not False:
        # Creo cursor 
        cursor = conn.cursor()

        # Obtengo los eventos ya cargados en la tabla
        cursor.execute(f"""
                        SELECT DISTINCT(timestamp_event) FROM {table_name}  
                        """)
        
        event_check = cursor.fetchall()

        if any([timestamp == date[0] for date in event_check]):
            print('El evento ya se encontraba guardado en la tabla')
        
        else:
            # Variables para crear la inserción a la base de datos
            columns = ", ".join(data.columns)
            s = ", ".join( ['%s'] * len(data.columns) )

            # Inserción de valores SQL
            sql = f"""
                INSERT INTO {table_name} ({columns})
                VALUES ({s}) """

            # Valores a ingestar la base de datos
            values = [tuple(row) for row in data.values]
            
            try:
            # Bucle de inserción por filas
                for val in values:
                    cursor.execute(sql, val)
                    conn.commit()

                print(f"Se ingresaron {len(data)} registros en la tabla '{table_name}'")
            except:
                print('Hubo un error al momento de ingresar los datos a la base de datos')
        
        cursor.close()
    
    elif data:
        print("Debe contecarse a una base de datos para ingestar valores a la tabla")
    
    else:
        print("El input 'data' debe ser tipo DataFrame")


def ETL(client_id, client_secret, conn):
    # Extraction: Request a la API
    data_json = request_api(client_id, client_secret)
        
    # Transformation: Normalido el json y obtengo el timestamp del request
    data, timestamp = normalize_json(data_json)

    # Loading: Ingesta de datos obtenidos desde la API a la tabla 'subte'
    data_ingestion(conn, data, timestamp, 'subte')

    return data_json, data 