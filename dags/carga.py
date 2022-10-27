from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

def renombrar_columnas_paises():
    df2 = pd.read_parquet(
        'data/datos_pre_procesados/paises_del_mundo.parquet'
    )
    df2.reset_index(inplace=True)   
    
    df2.rename(
        columns={
            "id": "Iso3",
        },
        inplace=True,
    )

    df2.to_parquet(
        'data/datos_pre_procesados/temp_df2.parquet'
    )

def crear_columnas_indicadores():
   
    df1 = pd.read_parquet(
        'data/datos_pre_procesados/df_unpd_&_twb.parquet'
    )

    df2 = pd.read_parquet(
        'data/datos_pre_procesados/temp_df2.parquet'
    )

    df_copy_df1 = (
    df1[["countryiso3code", "date", "INB_percapita", "nivel_ingreso"]]
    .copy()
    .replace("", np.nan)
    )

    df_copy_df1[["nivel_ingreso_codes"]] = (
        df_copy_df1[["nivel_ingreso"]]
        .apply(lambda col: pd.Categorical(col).codes)
        .replace(-1, np.nan)
    )
    df_copy_df1[["countryiso3code_codes"]] = (
        df_copy_df1[["countryiso3code"]]
        .apply(lambda col: pd.Categorical(col).codes)
        .replace(-1, np.nan)
    )

    df_copy_df1.rename(
        columns={
            "countryiso3code": "Iso3",
        },
        inplace=True,
    )

    df_copy_df1_merge_df2 = pd.merge(df_copy_df1, df2, how="inner", on="Iso3")
    
    # Creación de tablas temporales
    df_copy_df1_merge_df2.to_parquet(
        'data/datos_pre_procesados/temp_merge.parquet'
    )

    df_copy_df1.to_parquet(
        'data/datos_pre_procesados/temp_copy_df1.parquet'
    )

def crear_ingresos():

    df_copy_df1_merge_df2 = pd.read_parquet(
        'data/datos_pre_procesados/temp_merge.parquet'
    )

    df_ingresos = df_copy_df1_merge_df2[
        ["countryiso3code_codes", "date", "INB_percapita", "nivel_ingreso_codes"]
    ].copy()

    df_ingresos.rename(
        columns={
            "countryiso3code_codes": "pais_id",
            "date": "año",
            "nivel_ingreso_codes": "nivel_id",
        },
        inplace=True,
    )

    # Creación de parquet final
    df_ingresos.to_parquet(
        'data/datos_procesados/ingreso.parquet'
    )

def crear_paises():

    df_copy_df1 = pd.read_parquet(
        'data/datos_pre_procesados/temp_copy_df1.parquet'
    )

    df2 = pd.read_parquet(
        'data/datos_pre_procesados/temp_df2.parquet'
    )

    df_paises = df_copy_df1[["countryiso3code_codes", "Iso3"]].copy()
    df_paises.drop_duplicates(inplace=True, ignore_index=True)
    df_paises.rename(
        columns={
            "countryiso3code_codes": "id",
        },
        inplace=True,
    )

    df_paises_merge_df2 = pd.merge(df_paises, df2, how="left", on="Iso3")
    df_paises_merge_df2.dropna(inplace=True)
    df_paises_merge_df2

    df_paises_agregados = df_paises_merge_df2[
        ["id", "iso2Code", "Iso3", "name", "longitude", "latitude", "region.value"]
    ].copy()

    df_paises_agregados.rename(
        columns={
            "name": "nombre",
            "iso2Code": "Iso2",
            "Longitude": "longitud",
            "Latitude": "latitud",
            "region.value": "región",
        },
        inplace=True,
    )

    df_paises_agregados.to_parquet(
        'data/datos_procesados/pais.parquet'
    )

def crear_niveles():

    df_copy_df1 = pd.read_parquet(
        'data/datos_pre_procesados/temp_copy_df1.parquet'
    )

    df_niveles = df_copy_df1[["nivel_ingreso_codes", "nivel_ingreso"]].copy()
    df_niveles.dropna(inplace=True)
    df_niveles.drop_duplicates(inplace=True, ignore_index=True)
    df_niveles.rename(
        columns={
            "nivel_ingreso_codes": "id",
            "nivel_ingreso": "cuartil",
        },
        inplace=True,
    )

    df_niveles.to_parquet(
        'data/datos_procesados/nivel.parquet'
    )

def crear_indices():

    df1 = pd.read_parquet(
        'data/datos_pre_procesados/df_unpd_&_twb.parquet'
    )

    df_paises_agregados = pd.read_parquet(
        'data/datos_procesados/pais.parquet'
    )

    df_copy_df2 = (
        df1[df1.columns.difference(["nivel_ingreso", "INB_percapita"])]
        .copy()
        .replace("", np.nan)
    )

    df_copy_df2[["countryiso3code_codes"]] = (
        df_copy_df2[["countryiso3code"]]
        .apply(lambda col: pd.Categorical(col).codes)
        .replace(-1, np.nan)
    )

    df_copy_df2.rename(
        columns={
            "countryiso3code": "Iso3",
        },
        inplace=True,
    )

    df_copy_df2_merge_df2 = pd.merge(
        df_copy_df2, df_paises_agregados, how="inner", on="Iso3"
    )

    df_indices = (
        df_copy_df2_merge_df2[
            df_copy_df2_merge_df2.columns.difference(
                [
                    "Iso3",
                    "nombre",
                    "longitude",
                    "latitude",
                    "región",
                    "countryiso3code_codes",
                ]
            )
        ]
        .copy()
        .reset_index()
    )

    df_indices.rename(
        columns={
            "index": "id",
            "id": "pais_id",
        },
        inplace=True,
    )

    df_indices.to_parquet(
        'data/datos_procesados/indice.parquet'
    )

def cargar_base_de_datos():

    df_ingresos = pd.read_parquet(
        'data/datos_procesados/ingreso.parquet'
    )

    df_paises_agregados = pd.read_parquet(
        'data/datos_procesados/pais.parquet'
    )

    df_niveles = pd.read_parquet(
        'data/datos_procesados/nivel.parquet'
    )

    df_indices = pd.read_parquet(
        'data/datos_procesados/indice.parquet'
    )

    # Remote Linode db
    DATABASE_URL = "postgresql://linpostgres:Hy38f5ah$vl5r1rD@lin-10911-2829-pgsql-primary.servers.linodedb.net:5432/postgres"
    engine = create_engine(DATABASE_URL)

    df_ingresos.to_sql(
        "ingreso", 
        con=engine, 
        index=True, 
        if_exists="replace", 
        index_label="id"
    )

    df_niveles.to_sql(
        "nivel", 
        con=engine, 
        index=False, 
        if_exists="replace", 
        index_label="id"
    )

    df_paises_agregados.to_sql(
        "pais", 
        con=engine, 
        index=False, 
        if_exists="replace", 
        index_label="id"
    )

    df_indices.to_sql(
        "indice", 
        con=engine, 
        index=False, 
        if_exists="replace", 
        index_label="id"
    )

default_arg = {
    'owner' : 'domingo',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=5)
}

with DAG (
    default_args=default_arg,
    dag_id='pruebas_de_carga_v0.1.2',
    start_date=datetime(2022, 10, 25),
    schedule_interval='@daily',
    catchup=True
) as dag:

    renombrar = PythonOperator(
        task_id= 'Renombrar_columas_tabla_paises',
        python_callable=renombrar_columnas_paises
    )

    columnas = PythonOperator(
        task_id= 'Crear_columnas_y_tablas_temporales',
        python_callable=crear_columnas_indicadores
    )

    ingresos = PythonOperator(
        task_id = 'Crear_parquet_ingreso',
        python_callable=crear_ingresos
    )

    paises = PythonOperator(
        task_id = 'Crear_parquet_pais',
        python_callable=crear_paises
    )

    niveles = PythonOperator(
        task_id = 'Crear_parquet_nivel',
        python_callable=crear_niveles
    )

    indices = PythonOperator(
        task_id = 'Crear_parquet_indice',
        python_callable=crear_indices
    )

    remover = BashOperator(
        task_id='Eliminar_archivos_temporales',
        bash_command='rm /opt/airflow/data/datos_pre_procesados/temp*.parquet'
    )

    carga = PythonOperator(
        task_id='Carga_base_de_datos',
        python_callable=cargar_base_de_datos
    )

    renombrar >> columnas
    columnas >> [ingresos, paises, niveles, indices]
    [ingresos, paises, niveles, indices] >> remover >> carga