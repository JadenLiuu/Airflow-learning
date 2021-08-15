from airflow import DAG

from datetime import datetime
from tasks.xcom_tasks import *


# one DAG will write to Xcom
default_args = {
    'start_date': datetime(2021,8,15)
}

with DAG('xcom_dag', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:
    NUM_MODELS = 3
    
    # downloading_data -> processing_tasks -> choose_best_model
    downloading_data = get_download_task()
    processing_tasks = get_tasks_of_models(NUM_MODELS)
    best_model, accurate, inaccurate = choose_best_model(NUM_MODELS)
    storing_model = get_storing_task()

    downloading_data >> processing_tasks >> best_model >> [accurate, inaccurate] >> storing_model