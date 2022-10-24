from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "coder2j",
    "retry": 5,
    "retry_delay": timedelta(minutes=5),
}


def get_sklearn():
    import sklearn

    print(f"sklearn with version: {sklearn.__version__} ")


def get_matplotlib():
    import matplotlib

    print(f"matplotlib with version: {matplotlib.__version__}")


def get_streamlit():
    import streamlit

    print(f"streamlit with version: {streamlit.__version__}")


with DAG(
    default_args=default_args,
    dag_id="dag_with_python_dependencies_v04",
    start_date=datetime(2022, 10, 22),
    schedule_interval="@daily",
) as dag:
    task1 = PythonOperator(
        task_id="get_sklearn",
        python_callable=get_sklearn,
    )

    task2 = PythonOperator(
        task_id="get_matplotlib",
        python_callable=get_matplotlib,
    )

    task3 = PythonOperator(
        task_id="get_streamlit",
        python_callable=get_streamlit,
    )

    task1 >> task2 >> task3
