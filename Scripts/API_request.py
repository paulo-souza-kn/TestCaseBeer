#Creator: Souza Paulo
#Creation date: 2024-05-29
#Description: Read an API with beers datas 

#Step 1: Import main libs to work
import pandas as pd
import requests
from datetime import datetime
import os
import warnings
warnings.filterwarnings("ignore")

#Step 2: create a function to request the data and save then to historic folder
def save_api_response_as_csv(url, historicfolder):
    try:
        response = requests.get(url, verify=False)
        # If success, save the data into csv archive for historic
        if response.status_code == 200:
            filename_prefix = 'Beers'
            filename = '{}_{}.csv'.format(filename_prefix, datetime.now().strftime('%Y_%m_%d_%H%M%S'))
            data = pd.DataFrame(response.json())
            data.to_csv(os.path.join(historicfolder, filename), index=False)
            print("Data saved successfully as '{}'".format(filename))
            return True
        else:
            print("Error in request:", response.status_code)
            return False
    except Exception as e:
        print("Error:", e)
        return False

url = 'https://api.openbrewerydb.org/breweries'
historicfolder = r'C:\Users\paulo.souza\Desktop\Dados&Desenvolvimento\Aplicacao\Beers\HistoricFolder\RawData'
save_api_response_as_csv(url, historicfolder)