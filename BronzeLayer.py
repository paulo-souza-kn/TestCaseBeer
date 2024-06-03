#Creator: Souza Paulo
#Creation date: 2024-05-29
#Description: Ingest bronze layer with beer data

#Step 1: Import main libs to work
import pandas as pd
import requests
from datetime import datetime
import os
import pyodbc
import warnings
warnings.filterwarnings("ignore")
import glob

#Step 2:Create SQL connection variables
server = 'BeersServerTest'
database = 'db_bronze'
username = 'BeerUser'
password = 'BeerUser'
driver = '{ODBC Driver 17 for SQL Server}'

#Stet 4: Create folder variables
FolderPath = r'C:\Users\paulo.souza\Desktop\Dados&Desenvolvimento\Aplicacao\Beers\HistoricFolder\RawData'
BronzeLayer = r'C:\Users\paulo.souza\Desktop\Dados&Desenvolvimento\Aplicacao\Beers\HistoricFolder\BronzeLayer' #Simulate a Datable into DB
filename = 'BronzeLayer_Beers_' + datetime.now().strftime('%Y_%m_%d_%H%M%S') + '.csv'
TableName = 'dbo.BeerBronzeLayer'

#Step 3: Open the most recently data downloaded
def GetMostRecentFile(folder_path):
    try:
        # Obtém a lista de todos os arquivos na pasta
        files = glob.glob(os.path.join(folder_path, '*'))
        if not files:
            print("A pasta está vazia.")
            return None
        most_recent_file = max(files, key=os.path.getmtime)
    
        print("Arquivo mais recente", most_recent_file)
        
        return most_recent_file
    except Exception as e:
        print("Erro:", e)
        return None

def CreateBronzeLayerData(most_recent_file):
    try:
        data = pd.read_csv(most_recent_file, dtype=str)
        data['LineHash'] = data['id']
        data['BlockHash'] = 'Hash'
        data['FileName'] = filename
        data['BronzeDirectory'] = FolderPath
        data['InsertDate'] = datetime.now()
        data.drop(columns=['id'])
        data = data.astype(str)
    except Exception as e:
        print("Erro:", e)
    
    #Save historic file
    data.to_csv(os.path.join(BronzeLayer,filename), index=False)
    print('Arquivo criado com sucesso')
    
def InsertDataIntoBronzeLayer(data, TableName):
    try:
        # Cria a string de conexão
        connection_string = f'DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password}'

        # Estabelece a conexão
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        
        
        for index, row in data.iterrows():
            columns = ', '.join(row.index)
            values = ', '.join(['?'] * len(row))
            sql = f"INSERT INTO {TableName} ({columns}) VALUES ({values})"
            cursor.execute(sql, tuple(row))
        
        conn.commit()
        cursor.close()

        # Fechar a conexão
        conn.close()
    except Exception as e:
        print("Erro:", e)
    
MostRecentFile = GetMostRecentFile(FolderPath)
CreateBronzeLayerData(MostRecentFile)

# Inserir o DataFrame na tabela
InsertDataIntoBronzeLayer(os.path.join(BronzeLayer,filename),TableName)





