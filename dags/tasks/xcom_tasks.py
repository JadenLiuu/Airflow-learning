from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from random import uniform


def __training_model(ti):
    acc = uniform(0.0, 100.0)
    print(f'model\'s accuracy: {acc}')
    ## return `acc` in each tasks, passing data via xcom automaticly
    # return acc
    ti.xcom_push(key='model_accuracy', value=acc)


def __choose_best_model(ti, num_models):
    print('Choose best model')
    ls_taskid = [f'training_models.train_model_{i+1}' for i in range(num_models)]
    accs = ti.xcom_pull(key='model_accuracy', task_ids=ls_taskid)
    best_acc = max(accs)
    print(f'Best acc = {best_acc:0.2f}%')
    if best_acc > 80.0:
        return 'accurate' # The next task_id should be `accurate`
    else:
        return 'inaccurate' # The next task_id should be `inaccurate`


def get_download_task():
    return BashOperator(
        task_id='downloading_data',
        bash_command='sleep 1',
        do_xcom_push=False
    )

def get_tasks_of_models(num_models):
    with TaskGroup('training_models') as group:
        for i in range(num_models):
            traing_model = PythonOperator(
                task_id = f'train_model_{i+1}',
                python_callable=__training_model
            )
        return group


def choose_best_model(num_models):
    best_model = BranchPythonOperator(
        task_id = f'best_model',
        python_callable=__choose_best_model,
        op_kwargs={'num_models': num_models}
    )

    accurate = DummyOperator(task_id='accurate', do_xcom_push=False)
    inaccurate = DummyOperator(task_id='inaccurate', do_xcom_push=False)

    return best_model, accurate, inaccurate


def get_storing_task():
    storing_model = DummyOperator(
        task_id='storing_model',
        trigger_rule='none_failed_or_skipped'
    )
    return storing_model
