from airflow import DAG
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from tasks.parallel_tasks import get_group, get_tasks_of_task, get_tasks_in_chain


# The args we always defined: start_date, schedule_interval
default_args = {
    'start_date': datetime(2021,8,10)
}

with DAG('dag_id', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:

    task1 = DummyOperator(task_id='task1')
    tasks_processing = get_group()
    task4 = DummyOperator(task_id='task4')

    task1 >> tasks_processing >> task4;

with DAG('task_groups', schedule_interval='@daily', default_args=default_args, catchup=False) as exp:
    task_groups = get_tasks_of_task()

with DAG('task_groups_chain', schedule_interval='@daily', default_args=default_args, catchup=False) as exp:
    task_groups_chain = get_tasks_in_chain()
