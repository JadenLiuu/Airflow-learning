from airflow import DAG
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag import SubDagOperator
from subdags.dummy_parallel import dummy_parallel


# The args we always defined: start_date, schedule_interval
default_args = {
    'start_date': datetime(2021,8,10)
}

with DAG('dag_id', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:

    task1 = DummyOperator(task_id='task1')
    
    tasks_processing = SubDagOperator(
        task_id='tasks_processing',
        dag=dummy_parallel('dag_id', 'tasks_processing', default_args)
    )
    
    task4 = DummyOperator(task_id='task4')

    task1 >> tasks_processing >> task4;