from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
import requests
import pandas as pd


def consultar_twb(pais: str, indicador:str, pagina:int = 1):

    '''Función que contacta a la API y devuelve la respuesta
    que brinda la misma en formato json'''

    # Página de la api y path al recurso solicitado
    api_url = 'http://api.worldbank.org/v2/es'
    path = f'/country/{pais}/indicator/{indicador}'
    url = api_url + path

    # Creamos el diccionario con los parametros 
    # para el método get
    args= {
        "date":'1990:2020',
        'page':pagina,
        "per_page":1000,
        "format":"json",
        "prefix":"Getdata",
    }
   
    return requests.get(url,params=args).json()

def carga_incremental_twb(pais = 'all', indicador=''):
    '''Función que a partir de un país y un indicador 
    llama a consultar y establece qué tipo de contenido tiene
    según eso devuelve o no un dataframe con todos los datos'''

    consulta = consultar_twb(pais, indicador)
    try:
        # La primera parte de la respuesta nos indica en 
        # cuantas páginas de encuentra la información
        paginas = consulta[0]["pages"]

        # La segunda parte nos retorna una lista de 
        # diccionarios con la información que queríamos
        datos=consulta[1]

    except:
        print('No hay datos para:', indicador, pais)
        pass
    else:
        if paginas >= 1:
            # Agregamos los valores de las otras páginas a
            # nuestra lista de diccionarios
            for pagina in range(2,paginas+1):
                datos.extend(consultar_twb(pais, indicador, pagina)[1])

            # Creo el DataFrame con todos los datos
            data = pd.json_normalize(datos)
            return data
        return pd.DataFrame(['error'],columns=['no_data'])

def carga_twb():

    consultar_por = {
        'SP.DYN.LE00.IN': 'esperanza_vida_total',
        'SP.DYN.LE00.FE.IN': 'esperanza_vida_mujeres',
        'SP.DYN.LE00.MA.IN': 'esperanza_vida_varones',
        'NY.GDP.PCAP.PP.CD': 'pib_pc_prec_inter',
        'NY.GNP.PCAP.CD': 'INB_percapita',
        'SH.H2O.BASW.ZS': 'acceso_agua_potable(%)',
        'SH.STA.BASS.ZS': 'acceso_servicios_sanitarios(%)'
    }

    for indicador in consultar_por:
        datos = carga_incremental_twb(pais='all', indicador=indicador)
        # Guardo el dataframe resultante
        datos.to_csv(f'data/df_TWB_{indicador}.csv')
        print(f'Datos sobre {consultar_por[indicador]} guardados')

default_arg = {
    'owner' : 'domingo',
    'retries' : 3,
    'retry_delay' : timedelta(minutes=5)
}

with DAG (
    default_args=default_arg,
    dag_id='pruebas_de_carga_v0.0.10',
    start_date=datetime(2022, 10, 24),
    schedule_interval='@daily'
) as dag:
    tarea1 = PythonOperator(
        task_id='Carga_datos_banco_mundial',
        python_callable=carga_twb
    )

    tarea1