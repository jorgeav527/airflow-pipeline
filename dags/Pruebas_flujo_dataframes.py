from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd

def carga_inicial():
    df1 = pd.DataFrame()
    df2 = pd.DataFrame([['holiwis']],columns=['hola'])
    df1 = pd.concat([df1,df2], ignore_index=True)
    df1.to_csv('data/temp.csv', index=False)

def transformacion():
    df1 = pd.read_csv('data/temp.csv')
    df1.rename(columns={'hola':'saludo'},inplace=True)
    df1.to_csv('data/temp.csv', index=False)

def saludar():
    df1 = pd.read_csv('data/temp.csv')
    print(df1.iat[0,0])

default_arg = {
    'owner' : 'domingo',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=5)
}

with DAG (
    default_args=default_arg,
    dag_id='pruebas_de_flujo_v0.1.3',
    start_date=datetime(2022, 10, 25),
    schedule_interval='@daily',
    catchup=True
) as dag:

    carga = PythonOperator(
        task_id='carga_inicial',
        python_callable=carga_inicial
    )

    transformacion_datos = PythonOperator(
        task_id='Transformacion_datos',
        python_callable=transformacion
    )

    imprimir = PythonOperator(
        task_id='Saludo_de_exito',
        python_callable=saludar
    )

    eliminar_temporales = BashOperator(
        task_id='Eliminar_archivos_temporales',
        bash_command='rm /opt/airflow/data/temp.csv'
    )

    carga >> transformacion_datos >> imprimir >> eliminar_temporales