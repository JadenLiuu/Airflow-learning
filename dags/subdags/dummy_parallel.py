from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

# create the function to control subdag
def dummy_parallel(parent_id, child_id, default_args):
    with DAG(f'{parent_id}.{child_id}', default_args=default_args) as dag:
        task2 = DummyOperator(task_id='task2')
        task3 = DummyOperator(task_id='task3')

        return dag