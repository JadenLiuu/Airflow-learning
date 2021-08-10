from airflow.models import DAG
from datetime import datetime
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from pandas import json_normalize
import os
import json

# The args we always defined: start_date, schedule_interval
default_args = {
    'start_date': datetime(2021,8,8)
}

AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME')
TMP_USER_CSV = '/tmp/processed_ser.csv'
DAG_NAME = 'user_processing'

def _run_process_user(ti):
    users = ti.xcom_pull(task_ids=['extracting_user'])
    if not len(users) or 'results' not in users[0]:
        raise ValueError('User is Empty')
    user_list_from_api = users[0]['results']
    user = user_list_from_api[0]
    df_processed_user = json_normalize({
        'firstname': user['name']['first'],
        'lastname': user['name']['last'],
        'country': user['location']['country'],
        'username': user['login']['username'],
        'password': user['login']['password'],
        'email': user['email']
    })
    df_processed_user.to_csv(TMP_USER_CSV, index=None, header=False)


with DAG(DAG_NAME, schedule_interval='@daily', default_args=default_args, catchup=False) as dag:
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

    extracting_user = SimpleHttpOperator(
        task_id='extracting_user',
        http_conn_id='user_api',
        endpoint='api/',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

    processing_user = PythonOperator(
        task_id='processing_user',
        python_callable=_run_process_user
    )

    echo_cmd = f'.separator ","\n.import {TMP_USER_CSV} users'
    storing_user = BashOperator(
        task_id='storing_user',
        bash_command=f'echo -e "{echo_cmd}" | sqlite3 {AIRFLOW_HOME}/airflow.db'
    )

    # Define the order of the tasks!
    create_table >> is_api_available >> extracting_user >> processing_user >> storing_user;
