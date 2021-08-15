from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from random import uniform


def _training_model(ti):
    acc = uniform(1.0, 100.0)
    print(f'model\'s accuracy: {acc}')
    ## return `acc` in each tasks, passing data via xcom automaticly
    # return acc
    ti.xcom_push(key='model_accuracy', value=acc)


def _choose_best_model(ti, num_models):
    print('Choose best model')
    ls_taskid = [f'training_models.train_model_{i+1}' for i in range(num_models)]
    accs = ti.xcom_pull(key='model_accuracy', task_ids=ls_taskid)
    print(accs)
    ti.xcom_push(key='best_model', value=max(accs))


def _is_accurate(ti):
    best_acc = ti.xcom_pull(key='best_model', task_ids='get_best_model')
    print(f'Best acc = {best_acc:0.2f}%')
    if best_acc > 80.0:
        return ('accurate') # The next task_id should be `accurate`
    else:
        return ('inaccurate') # The next task_id should be `inaccurate`


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
                python_callable=_training_model
            )
        return group


def get_best_model(num_models):
    traing_model = PythonOperator(
        task_id = f'get_best_model',
        python_callable=_choose_best_model,
        op_kwargs={'num_models': num_models}
    )
    return traing_model


def braching():
    is_accurate = BranchPythonOperator(
        task_id='is_accurate',
        python_callable=_is_accurate,
        do_xcom_push=False
    )

    accurate = DummyOperator(task_id='accurate', do_xcom_push=False)
    inaccurate = DummyOperator(task_id='inaccurate', do_xcom_push=False)


    is_accurate >> [accurate, inaccurate]
    return is_accurate

