from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
import requests
import pandas as pd
import os

naciones_unidas = {
    '22': ['mort', 'tasa_mortalidad_infantil'],
    '54': ['pop', 'densidad_población_por_kilómetro_cuadrado)'],
    '65': ['imigrt', 'migración_neta_total'],
    '49': ['pop', 'población_total_por_sexo'],
    '60': ['mort', 'total_muertes_por_sexo'],
    '53': ['pop', 'tasa_bruta_cambio_natural_población'],
    '66': ['imigrt', 'tasa_bruta_migración_neta'],
    '72': ['pop', 'proporción_sexos_población_total'],
    '1': ['fam', 'prevalencia_anticonceptivos_porcentaje'],
    '67': ['pop', 'mediana_edad_población'],
    '59': ['mort', 'tasa_bruta_mortalidad_por_1000_habitantes'],
    '51': ['pop', 'tasa_bruta_variación_total_población'],
    '50': ['pop', 'cambio_de_la_población'],
    '41': ['pop', 'población_femenina_edad_reproductiva_(15-49 años)'],
    '24': ['mort', 'tasa_mortalidad_menores_cinco_años'],
    '52': ['pop', 'cambio_natural_población'],
    '19': ['fert', 'tasa_fertilidad'],
    '42': ['marstat', 'estado_civil_casado_porcentaje']
 }

banco_mundial = {
    'SP.DYN.LE00.IN': 'esperanza_vida_total',
    'SP.DYN.LE00.FE.IN': 'esperanza_vida_mujeres',
    'SP.DYN.LE00.MA.IN': 'esperanza_vida_varones',
    'SI.POV.GINI': 'índice_gini',
    'SE.XPD.TOTL.GD.ZS': 'gasto_púb_educacion_pje',
    'SE.COM.DURS': 'duración_educ_obligatoria',
    'NY.GDP.PCAP.CD': 'pib_pc_usd_actuales',
    'NY.GDP.MKTP.PP.CD': 'pib_ppa_prec_inter',
    'IQ.SCI.OVRL': 'capacidad_estadística',
    'SP.POP.TOTL.FE.ZS': 'población_mujeres_pje',
    'SP.POP.TOTL.MA.ZS': 'población_hombres_pje',
    'NY.GDP.PCAP.PP.CD': 'pib_pc_prec_inter',
    'AG.LND.FRST.ZS': 'porcentaje_de_bosque',
    'EN.ATM.CO2E.PC': 'emisiones_co2',
    'SH.XPD.CHEX.PC.CD': 'inversion_salud_percapita',
    'SH.MED.BEDS.ZS': 'camas_hospitales_c/1000personas',
    'SP.DYN.IMRT.IN': 'mortalidad_infantil_c/1000nacimientos',
    'SH.H2O.BASW.ZS': 'acceso_agua_potable(%)',
    'SH.STA.BASS.ZS': 'acceso_servicios_sanitarios(%)',
    'SH.STA.SUIC.P5': 'tasa_mortalidad_suicidio_c/100.000',
    'SL.UEM.TOTL.ZS': 'tasa_desempleo',
    'SP.URB.TOTL.IN.ZS': 'tasa_poblacion_urbana',
    'NY.GNP.PCAP.CD': 'INB_percapita',
    'PV.EST' :'estabilidad_política'
}

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

def tabla_paises():
    incomes = ['HIC','INX','LIC','LMC','LMY','MIC','UMC']
    trabajo = pd.DataFrame()

    for income in incomes:
        url = 'http://api.worldbank.org/v2/es/country' #Aqui vemos la lista de niveles de Ingreso y sus respectivos códigos
        args = {"format":"json", "prefix":"Getdata", "incomelevel":income}
        dic_Income_Level = requests.get(url, params=args)
        datos = dic_Income_Level.json()[1]
        data = pd.json_normalize(datos)
        trabajo = pd.concat([trabajo, data], ignore_index=True)

    trabajo.replace("", float("NaN"), inplace=True)
    trabajo.dropna(subset=['longitude','latitude'],inplace=True)

    # Sección para conseguir el nombre de todos los países 
    # con su respectiva ubicación
    temp = trabajo[['id',
                    'iso2Code',
                    'name',
                    'longitude',
                    'latitude',
                    'region.id',
                    'region.value']]

    Agregar = pd.DataFrame([['USA',
                            'US',
                            'Estados Unidos',
                            '-95.712891',
                            '37.0902',
                            'NAC',
                            'América del Norte']],
                            columns=['id',
                            'iso2Code',
                            'name',
                            'longitude',
                            'latitude',
                            'region.id',
                            'region.value'])
                            
    temp = pd.concat([temp, Agregar], ignore_index=True)
    temp = temp.groupby('id').agg(pd.Series.mode)
    temp.reset_index(inplace=True)
    temp.to_parquet('data/datos_pre_procesados/paises_del_mundo.parquet')

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
        return pd.DataFrame([['error']],columns=['no_data'])

def carga_twb():

    for indicador in banco_mundial:
        datos = carga_incremental_twb(pais='all', indicador=indicador)
        # Guardo el dataframe resultante
        datos.to_parquet(f'data/datos_brutos/df_TWB_{indicador}.parquet')
        print(f'Datos sobre {banco_mundial[indicador]} guardados')

def codigo_paises() -> str:
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
    country = codigo_paises() 
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

    for indicador in naciones_unidas:
        datos = carga_incremental_unpd(indicador)
        datos.to_parquet(
            f'data/datos_brutos/df_UNPD_{naciones_unidas[indicador][0]}_{indicador}.parquet',
            index=False
        )
        print(f'Datos sobre {naciones_unidas[indicador][1]} guardados')

def creacion_directorios():
    os.makedirs('data/datos_pre_procesados', exist_ok=True)
    os.makedirs('data/datos_procesados', exist_ok=True)
    os.makedirs('data/datos_brutos', exist_ok=True)


default_arg = {
    'owner' : 'domingo',
    'retries' : 3,
    'retry_delay' : timedelta(minutes=5)
}

with DAG (
    default_args=default_arg,
    dag_id='Carga_de_datos_v0.2.1',
    start_date=datetime(2022, 10, 31),
    schedule_interval='@daily'
) as dag:
    twb = PythonOperator(
        task_id='Carga_datos_banco_mundial',
        python_callable=carga_twb
    )

    unpd = PythonOperator(
        task_id='Carga_datos_naciones_unidas',
        python_callable=carga_unpd
    )

    paises = PythonOperator(
        task_id='Carga_lista_de_paises',
        python_callable=tabla_paises
    )

    directorios= PythonOperator(
        task_id='Creación_de_directorios',
        python_callable=creacion_directorios
    )

    directorios >> paises >> twb >> unpd