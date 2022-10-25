import requests
import pandas as pd

def paises() -> str:
    # Define target URL.
    base_url = "https://population.un.org/dataportalapi/api/v1/locationsWithAggregates?pageNumber=1"

    # Call the API and convert the resquest into JSON object.
    response = requests.get(base_url).json()

    # Convert JSON object to data frame.
    df = pd.json_normalize(response)

    # Converts call into JSON and concat to the previous data frame.
    for page in range(2, 4):
        # Reset the target to the next page
        target = f"https://population.un.org/dataportalapi/api/v1/locationsWithAggregates?pageNumber={page}"

        # Each iteration call the API and convert the resquest into JSON object.
        response = requests.get(target).json()

        # Each iteration convert JSON object to data frame.
        df_temp = pd.json_normalize(response)

        # Each iteration concat the data frames.
        df = pd.concat([df, df_temp], ignore_index=True)
    
    # Stores indicator codes in a list
    id_code = [str(code) for code in df["Id"].values]

    # Converts indicator code list into string to be used in later API call
    id_code_string = ",".join(id_code)
    
    return id_code_string

def carga_incremental(indicator_code: int):
    base_url_UNPD = "https://population.un.org/dataportalapi/api/v1"
    country = paises()  # set the country code
    start_year = 1990  # set the start year
    end_year = 2020  # set the end year

    # define the target URL
    target = (
        base_url_UNPD
        + f"/data/indicators/{indicator_code}/locations/{country}/start/{start_year}/end/{end_year}"
    )

    response = requests.get(target)  # Call the API
    j = response.json()  # Format response as JSON
    df_UNPD = pd.json_normalize(j["data"])  # Read JSON data into dataframe

    # As long as the response contains information in the 'nextPage' field, the loop will continue to download and append data
    while j["nextPage"] is not None:
        response = requests.get(j["nextPage"])
        j = response.json()
        df_temp = pd.json_normalize(j["data"])
        df_UNPD = pd.concat([df_UNPD, df_temp], ignore_index=True)

    return df_UNPD

def carga():

    consultar_por ={
        24: "mort",
        22 : "mort",
        1: "fam",
        19: "fert"
    }

    for indicador in consultar_por:
        datos = carga_incremental(indicador)
        datos.to_parquet(f'df_UNPD_{consultar_por[indicador]}_{indicador}.parquet')
        print('Guardado')

carga()