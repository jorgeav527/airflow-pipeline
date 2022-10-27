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


def upload_file_to_bucket():
    """
    Subimos todos los archivos en formato parquet
    """
    for filename in os.listdir(directory_datos_brutos):
        f = os.path.join(directory_datos_brutos, filename)
        client.upload_file(
            Filename=f"{f}",
            Bucket="datos_brutos",
            Key=f"{filename}",
        )


# # for my_bucket_object in s3.Bucket("my_bucket").objects.filter(Prefix="user/folder/"):
# #     s3.Object(my_bucket_object.bucket_name, my_bucket_object.key).download_file(f'./aws/{my_bucket_object.key}'


# def download_file_to_bucket():
#     for filename in client.objects:
#         # f = os.path.join(directory_datos_brutos, filename)
#         # client.upload_file(
#         #     Filename=f"{f}",
#         #     Bucket="datos_brutos",
#         #     Key=f"{filename}",
#         # )
#         # client.download_file(
#         #     Bucket="datos_brutos",
#         #     Key="df_OMS_M_Est_cig_curr.parquet",
#         #     Filename="data/df_OMS_M_Est_cig_curr.parquet",
#         # )


with DAG(
    dag_id="upload_to_bucket_v1",
    start_date=datetime(2022, 10, 25),
    schedule_interval="@daily",
    default_args=default_args,
) as dag:
    task1 = PythonOperator(
        task_id="upload_files",
        python_callable=upload_file_to_bucket,
    )
    # task2 = PythonOperator(
    #     task_id="download",
    #     python_callable=download_file_to_bucket,
    # )
    task1
