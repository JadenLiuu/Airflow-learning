from airflow import DAG
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator


# The args we always defined: start_date, schedule_interval
default_args = {
    'start_date': datetime(2021,8,10)
}

with DAG('dag_id', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:

    task1 = DummyOperator(task_id='task1')
    task2 = DummyOperator(task_id='task2')
    task3 = DummyOperator(task_id='task3')
    task4 = DummyOperator(task_id='task4')

    task1 >> [task2, task3] >> task4;