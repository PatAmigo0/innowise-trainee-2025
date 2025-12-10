from datetime import datetime

from airflow.models.taskinstance import TaskInstance
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import chain, dag, task, task_group  # <-- Импортируем task_group


@task
def extract_data():
    return {"message": "HELLO"}


@task
def print_data(data: any):
    print("my data is: ", data)


def task4_callable(**context):
    ti: TaskInstance = context["ti"]
    ti.xcom_push("return_value", {"message": "haha!"})


def task5_callable(**context):
    ti: TaskInstance = context["ti"]
    data = ti.xcom_pull("py1")
    print("my second data is: ", data)


@task_group
def create_parallel_group(group_index: int):
    tasks = [EmptyOperator(task_id=f"task_{i}") for i in range(5)]
    chain(*tasks)


@dag(start_date=datetime(2025, 12, 7), schedule=None, catchup=False)
def test_dag_grouped():
    @task_group
    def main_task_group():
        task1 = EmptyOperator(task_id="test_task")
        task2 = BashOperator(task_id="test_task2", bash_command="echo HOORAY")

        task3 = print_data(extract_data())

        task4 = PythonOperator(task_id="py1", python_callable=task4_callable)
        task5 = PythonOperator(task_id="py2", python_callable=task5_callable)

        chain(task1, task2, task3, task4, task5)

    main_task = main_task_group()
    (
        main_task
        >> [
            create_parallel_group.override(group_id=f"batch_{j}")(group_index=j)
            for j in range(5)
        ]
        >> print_data(extract_data())
    )


test_dag_grouped()
