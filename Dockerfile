FROM apache/airflow:2.5.1

# Creo carpetar necesarias para Airflow
RUN mkdir -p /opt/airflow/dags\
    && mkdir -p /opt/airflow/plugins\
    && mkdir -p /opt/airflow/logs\
    && mkdir -p /opt/airflow/data/forecast\
    && mkdir -p /opt/airflow/data/static

# Inicializar la base de datos de Airflow
RUN airflow db init
# Creo el usuario admin
RUN airflow users create --username admin --firstname Admin --lastname User --role Admin --email valentintafura@hotmail.com --password admin

# Copio files al contenedor
COPY functions.py /opt/airflow/dags
COPY main.py /opt/airflow/dags
COPY config.py /opt/airflow/dags
COPY delays.csv /opt/airflow/data/forecast

COPY airflow.cfg /opt/airflow/airflow.cfg

COPY requirements.txt /opt/airflow

# Establezco el directorio de trabajo
WORKDIR /opt/airflow

# Instalo las dependencias
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Exponer el puerto
EXPOSE 8080

# Comando para ejecutar la aplicación
CMD ["airflow", "standalone"]