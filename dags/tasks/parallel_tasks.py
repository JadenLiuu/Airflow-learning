from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator


def get_group():
    with TaskGroup('tasks_processing') as tasks_processing:
        task2 = DummyOperator(task_id='task2')
        task3 = DummyOperator(task_id='task3')

        return tasks_processing

def get_tasks_of_task():
    
    with TaskGroup('tasks_processing') as tasks_of_groups:
        group = list()
        with TaskGroup('group1') as group1:
            task1 = DummyOperator(task_id='task1')
            task2 = DummyOperator(task_id='task2')
            task3 = DummyOperator(task_id='task3')
            task1 >> [task2, task3]
            group.append(group1)

        with TaskGroup('group2') as group2:
            task1 = DummyOperator(task_id='task1')
            task2 = DummyOperator(task_id='task2')
            task1 >> task2
            group.append(group2)
        
        with TaskGroup('group3') as group3:
            task1 = DummyOperator(task_id='task1')
            task2 = DummyOperator(task_id='task2')
            task1 >> task2
            group.append(group3)

        [group[0], group[1]] >> group[2]
        return tasks_of_groups

def get_tasks_in_chain():
    groups = list()
    for g in range(1,3):
        with TaskGroup(f'group{g}') as group:
            ls_subgroup = []
            for sub_group in range(2):
                with TaskGroup(f'sub_group{sub_group+1}') as tmp:
                    task1 = DummyOperator(task_id='task1')
                    task2 = DummyOperator(task_id='task2')
                    task1 >> task2
                    ls_subgroup.append(tmp)
            task1 = DummyOperator(task_id='task1')
            task2 = DummyOperator(task_id='task2')
            task1 >> ls_subgroup >> task2
            groups.append(group)

    for i in range(1, len(groups)):
        groups[i-1] >> groups[i]

    return groups
