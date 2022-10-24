from email.policy import default
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "coder2j",
    "retries": 5,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="our_first_dag_v3",
    default_args=default_args,
    description="this is our first dag that we write",
    start_date=datetime(2022, 5, 10, 2),
    schedule_interval="@daily",
) as dag:
    task1 = BashOperator(
        task_id="first_task", bash_command="echo hello world, this is the first try"
    )
    task2 = BashOperator(
        task_id="second_task",
        bash_command="echo hey, Im task2 and will be running after task1",
    )
    task3 = BashOperator(
        task_id="thrid_task",
        bash_command="echo hey, Im task3 task after task1 and at the same time task2 is running",
    )

    task1.set_downstream(task2)
    task1.set_downstream(task3)
