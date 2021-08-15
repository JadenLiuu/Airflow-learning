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
    choose_best_model = get_best_model(NUM_MODELS)
    is_accurate = braching()

    downloading_data >> processing_tasks >> choose_best_model >> is_accurate