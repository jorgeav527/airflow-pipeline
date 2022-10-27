from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
import pandas as pd
import os

naciones_unidas = {
    "df_UNPD_mort_22" : "tasa_mortalidad_infantil",
    "df_UNPD_pop_54": "densidad_población_por_kilómetro_cuadrado)",
    "df_UNPD_imigrt_65": "migración_neta_total",
    "df_UNPD_pop_49": "población_total_por_sexo",
    "df_UNPD_mort_60": "total_muertes_por_sexo",
    "df_UNPD_pop_53": "tasa_bruta_cambio_natural_población",
    "df_UNPD_imigrt_66": "tasa_bruta_migración_neta",
    "df_UNPD_pop_72": "proporción_sexos_población_total",
    "df_UNPD_fam_1": "prevalencia_anticonceptivos_porcentaje",
    "df_UNPD_pop_67": "mediana_edad_población",
    "df_UNPD_mort_59": "tasa_bruta_mortalidad_por_1000_habitantes",
    "df_UNPD_pop_51": "tasa_bruta_variación_total_población",
    "df_UNPD_pop_50": "cambio_de_la_población",
    "df_UNPD_pop_41": "población_femenina_edad_reproductiva_(15-49 años)",
    "df_UNPD_mort_24": "tasa_mortalidad_menores_cinco_años",
    "df_UNPD_pop_52": "cambio_natural_población",
    "df_UNPD_fert_19": "tasa_fertilidad",
    "df_UNPD_marstat_42": "estado_civil_casado_porcentaje",
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

problemas = [
    'df_UNPD_mort_22', 
    'df_UNPD_mort_24', 
    'df_UNPD_mort_60',
    'df_UNPD_pop_49'    
]

def lectura_y_transformacion():
    
    directorio = 'data/datos_brutos/'
    with os.scandir(directorio) as ficheros:
        # Tomamos unicamente la fecha y el iso3 para usarlo como indice
        df_twb=pd.read_parquet('data/datos_brutos/df_TWB_SP.DYN.LE00.IN.parquet')[['date','countryiso3code']]
        
        df_unpd = pd.read_parquet('data/datos_brutos/df_TWB_SP.DYN.LE00.IN.parquet')[['date','countryiso3code']]
        df_unpd.set_index(['countryiso3code', 'date'], inplace=True)

        for fichero in ficheros:
            if fichero.name.startswith('df_TWB'):
                # obtengo el cógigo de indicador que se encuentra en el nombre del fichero
                codigo_fichero = fichero.name[7:-8]
                # busco el código en mi lista de códigos 
                # y procedo a renombrar la columna de interés
                    
                df=pd.read_parquet(directorio+fichero.name)

                df_twb[banco_mundial[codigo_fichero]]=df.value
            
            elif fichero.name.startswith('df_UNPD'):
                codigo_fichero = fichero.name[:-8]
                    
                if codigo_fichero in problemas:
                    temp=pd.read_parquet(directorio+fichero.name)
                    
                    # Creo 3 tablas según el sexo sea hombre, mujer o ambos 
                    # y selecciono las columnas de interés
                    temp_male=temp.loc[temp.sex == "Male", ['iso3','timeLabel','value']]
                    temp_female=temp.loc[temp.sex == "Female", ['iso3','timeLabel','value']]
                    temp_both=temp.loc[temp.sex == "Both sexes", ['iso3','timeLabel','value']]

                    # Renombro la columna de interés según el diccionario
                    temp_both.rename(columns={"value":f"{naciones_unidas[codigo_fichero]}_ambos"}, inplace=True)
                    temp_male.rename(columns={"value":f"{naciones_unidas[codigo_fichero]}_masc"}, inplace=True)
                    temp_female.rename(columns={"value":f"{naciones_unidas[codigo_fichero]}_fem"}, inplace=True)

                    # Asigno un index multiple
                    temp_male.set_index(['iso3','timeLabel'], inplace=True)
                    temp_female.set_index(['iso3','timeLabel'], inplace=True)
                    temp_both.set_index(['iso3','timeLabel'], inplace=True)

                    df_unpd = df_unpd.join(temp_both,
                        on=['countryiso3code','date']
                        )

                    df_unpd = df_unpd.join(temp_male,
                        on=['countryiso3code','date']
                        )

                    df_unpd = df_unpd.join(temp_female,
                        on=['countryiso3code','date']
                        )

                else:
                    temp=pd.read_parquet(directorio+fichero.name)

                    temp.set_index(['iso3','timeLabel'], inplace=True)

                    temp.rename(columns={"value":naciones_unidas[codigo_fichero]}, inplace=True)
                    
                    df_unpd = df_unpd.join(temp[[naciones_unidas[codigo_fichero]]], 
                                            on=['countryiso3code','date'])

        # Eliminamos todos los valores nulos que existen en unpd                                    
        df_unpd.dropna(how='all',inplace=True)

        # Preparamos el dataframe para unirlo
        df_twb.set_index(['countryiso3code','date'], inplace=True)

        #unimos los dataframes en una sola tabla
        tabla = df_twb.join(df_unpd,on=['countryiso3code','date'])
    
    tabla.to_parquet('data/datos_pre_procesados/df_unpd_&_twb.parquet')


def transformaciones_finales():

    tabla = pd.read_parquet('data/datos_pre_procesados/df_unpd_&_twb.parquet')

    # Se etiqueta los países según su ingreso nacional por año
    tabla['nivel_ingreso'] = pd.cut(tabla['INB_percapita'],
                bins=[0,1025,3995,12375,200000],
                labels=['Ingreso Bajo', 
                        'Ingreso medio bajo', 
                        'Ingreso medio alto',
                        'Ingreso Alto'],
                include_lowest = True)
    
    # Se eliminan todos los nulos que existan sobre esperanza de vida
    tabla.dropna(subset=['esperanza_vida_total'], inplace=True)

    tabla.reset_index(inplace=True)

    tabla.to_parquet('data/datos_pre_procesados/df_unpd_&_twb.parquet')


default_arg = {
    'owner' : 'domingo',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=5)
}

with DAG (
    default_args=default_arg,
    dag_id='pruebas_de_transformacion_v0.2.2',
    start_date=datetime(2022, 10, 25),
    schedule_interval='@daily',
    catchup=True
) as dag:
    lectura = PythonOperator(
        task_id='Lectura_y_transformacion_de_datos',
        python_callable=lectura_y_transformacion
    )

    retoques = PythonOperator(
        task_id='Agregación_nuevos_datos',
        python_callable=transformaciones_finales
    )
    

    lectura >> retoques