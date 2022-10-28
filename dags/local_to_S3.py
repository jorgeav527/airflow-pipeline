from datetime import datetime, timedelta
from urllib import request
import os
import dotenv


from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests

# from s3_upload import pushS3
import boto3

linode_obj_config = {
    "aws_access_key_id": "S9154ZP0MM9NP88BX7YO",
    "aws_secret_access_key": "y8tvmknU9jbShBybAhFG7LJiRjARUKZxpCeL4vND",
    "endpoint_url": "https://bucket-airflow.us-southeast-1.linodeobjects.com",
}
default_args = {
    "owner": "jorgeav527",
    "retries": 5,
    "retry_delay": timedelta(minutes=10),
}
client = boto3.client("s3", **linode_obj_config)
directory_datos_brutos = "data/datos_brutos/"
directory_datos_pre_procesados = "data/datos_pre_procesados/"
directory_datos_procesados = "data/datos_procesados/"


def upload_file_to_bucket_datos_brutos():
    """
    Subimos todos los archivos en formato parquet
    """
    for filename in os.listdir(directory_datos_brutos):
        f = os.path.join(directory_datos_brutos, filename)
        client.upload_file(
            Filename=f"{f}",
            Bucket="datos_brutos",
            Key=f"{filename}",
            ExtraArgs={"ACL": "public-read"},
        )


def upload_file_to_bucket_datos_pre_procesados():
    """
    Subimos todos los archivos en formato parquet
    """
    for filename in os.listdir(directory_datos_pre_procesados):
        f = os.path.join(directory_datos_pre_procesados, filename)
        client.upload_file(
            Filename=f"{f}",
            Bucket="datos_pre_procesados",
            Key=f"{filename}",
            ExtraArgs={"ACL": "public-read"},
        )


def upload_file_to_bucket_datos_procesados():
    """
    Subimos todos los archivos en formato parquet
    """
    for filename in os.listdir(directory_datos_procesados):
        f = os.path.join(directory_datos_procesados, filename)
        client.upload_file(
            Filename=f"{f}",
            Bucket="datos_procesados",
            Key=f"{filename}",
            ExtraArgs={"ACL": "public-read"},
        )


with DAG(
    dag_id="upload_to_bucket_v6",
    start_date=datetime(2022, 10, 27),
    schedule_interval="@once",
    default_args=default_args,
) as dag:
    task1 = PythonOperator(
        task_id="upload_file_to_bucket_datos_brutos",
        python_callable=upload_file_to_bucket_datos_brutos,
    )
    task2 = PythonOperator(
        task_id="upload_file_to_bucket_datos_pre_procesados",
        python_callable=upload_file_to_bucket_datos_pre_procesados,
    )
    task3 = PythonOperator(
        task_id="upload_file_to_bucket_datos_procesados",
        python_callable=upload_file_to_bucket_datos_procesados,
    )
    task1 >> task2 >> task3
