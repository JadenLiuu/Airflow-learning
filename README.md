# Airflow-learning
## Dags
### user_processing
- Airflow learning with some basic operators
    - BashOperator
    - SqliteOperator
    - HttpSensor & SimpleHttpOperator
    - PythonOperator
- Building the simplest flow in airflow.


### dummy_parallel
- Airflow learning with subdag or taskgroup
- Contruct airflow with following DAGs (using dummyOperator to practice):
    1. [Pipeline 1](./images/1.png)
    2. [Pipeline 2](./images/2.png)
    3. [Pipeline 3](./images/3.png)


### xcom_dag
- Learn to exchance SMALL data between airflow tasks.
    ```python
        ti.xcom_push(key=[xcom_key], value=[value to be stored])
        ti.xcom_pull(key=[xcom_key], task_ids=ls_taskid)
    ```
- Bash operator would store xcom automatically, to cancel the storing action
    - add `do_xcom_push=False` in the task.
- learn `BranchPythonOperator` and 'trigger_rule'


## Docker-compose
- Require 3 containers to compose the airflow service.
    - Webserver
    - DB
    - Scheduler
> `wget http://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml`
- This file defines the required services we need to compose an airflow app.
    - Test it via: 
        1. cd airflow-local
        2. docker-compose -f docker-compose.yaml up -d
    - Then you could see your service with `docker ps` and use airflow UI on localhost:8080 in your browser.

### Celery
- Default downloaded docker-compose.yaml uses celery executor.

### Local
- See airflow-local/docker-compose-local.yaml