from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

def renombrar_columnas():

    df2.rename(
    columns={
        "id": "Iso3",
    },
    inplace=True,
    )
    pass
