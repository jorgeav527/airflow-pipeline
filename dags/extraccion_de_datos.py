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
        'SH.STA.BASS.ZS': 'acceso_servicios_sanitarios(%)',
        'PV.EST' :'estabilidad_política'
    }

    for indicador in consultar_por:
        datos = carga_incremental_twb(pais='all', indicador=indicador)
        # Guardo el dataframe resultante
        datos.to_csv(f'data/df_TWB_{indicador}.csv')
        print(f'Datos sobre {consultar_por[indicador]} guardados')


def paises() -> str:
    # Define la URL objetivo.
    base_url = "https://population.un.org/dataportalapi/api/v1/locationsWithAggregates?pageNumber=1"

    # Llama a la API y convierte la respuesta en un objeto JSON
    response = requests.get(base_url).json()

    # Convierte el objeto JSON en un DataFrame
    df = pd.json_normalize(response)

    # convierte la llamada en un objeto JSON y lo concatena a lo anterior
    for page in range(2, 4):
        # Reinicia el target a la siguiente página
        target = f"https://population.un.org/dataportalapi/api/v1/locationsWithAggregates?pageNumber={page}"

        # En cada iteración Llama a la API y convierte la respuesta en un objeto JSON
        response = requests.get(target).json()

        # En cada iteración Convierte el objeto JSON en un DataFrame
        df_temp = pd.json_normalize(response)

        # En cada iteración concatena los dataframes
        df = pd.concat([df, df_temp], ignore_index=True)
    
    # Guarda el cógido de los países en una lista
    id_code = [str(code) for code in df["Id"].values]

    # Convierte el la lista anterior en un string para ser usado luego
    id_code_string = ",".join(id_code)
    
    return id_code_string


def carga_incremental_unpd(indicator_code: int):
    base_url_UNPD = "https://population.un.org/dataportalapi/api/v1"
    country = paises() 
    start_year = 1990  
    end_year = 2020 

    target = (
        base_url_UNPD
        + f"/data/indicators/{indicator_code}/locations/{country}/start/{start_year}/end/{end_year}"
    )

    response = requests.get(target)  
    j = response.json()  
    df_UNPD = pd.json_normalize(j["data"]) 

    # Mientras la respuesta contenga información en el campo 'nextPage',
    # el loop continuará descargando datos y agregando a lo anterior
    while j["nextPage"] is not None:
        response = requests.get(j["nextPage"])
        j = response.json()
        df_temp = pd.json_normalize(j["data"])
        df_UNPD = pd.concat([df_UNPD, df_temp], ignore_index=True)

    return df_UNPD


def carga_unpd():

    consultar_por ={
        24: "mort",
        22 : "mort",
        1: "fam",
        19: "fert"
    }

    for indicador in consultar_por:
        datos = carga_incremental_unpd(indicador)
        datos.to_parquet(f'data/df_UNPD_{consultar_por[indicador]}_{indicador}.parquet')
        print(f'Datos sobre {consultar_por[indicador]} guardados')


default_arg = {
    'owner' : 'domingo',
    'retries' : 3,
    'retry_delay' : timedelta(minutes=5)
}

with DAG (
    default_args=default_arg,
    dag_id='pruebas_de_carga_v0.1.0',
    start_date=datetime(2021, 10, 24),
    schedule_interval='0 3 1 4 *'
) as dag:
    twb = PythonOperator(
        task_id='Carga_datos_banco_mundial',
        python_callable=carga_twb
    )

    unpd = PythonOperator(
        task_id='Carga_datos_naciones_unidas',
        python_callable=carga_unpd
    )

    [twb, unpd]