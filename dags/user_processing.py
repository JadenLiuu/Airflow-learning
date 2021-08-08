from airflow.models import DAG
from datetime import datetime
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.http.sensors.http import HttpSensor
import os

default_args = {
    'start_date': datetime(2021,8,8)
}

AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME')

with DAG('user_processing', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:
    # Define task - create table
    sql_command = ''
    with open(f'{AIRFLOW_HOME}/sql/user_processing_create_table.sql', 'r') as fs:
        sql_command = fs.read()

    create_table = SqliteOperator(
        task_id='create_table',
        sqlite_conn_id='db_sqlite',
        sql=sql_command
    )

    # Define task - is API available?
    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='user_api',
        endpoint='api/'
    )

