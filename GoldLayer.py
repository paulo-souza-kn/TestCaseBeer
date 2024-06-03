# Creator: Souza Paulo
# Creation date: 2024-05-29
# Description: Ingest Silver layer with beer data

# Step 1: Import main libs to work
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
database = 'db_gold'
username = 'BeerUser'
password = 'BeerUser'
driver = '{ODBC Driver 17 for SQL Server}'

# Cria a string de conexão
connection_string = f'DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password}'

# Estabelece a conexão
conn = pyodbc.connect(connection_string)

# Step 3: Função para ler, transformar e inserir dados
def InsertWithProcedure(Procedure, TargetTable):
    try:
        # Ler a tabela de origem
        query = f"EXEC {Procedure}"
        # Inserir os dados transformados na tabela de destino
        cursor = conn.cursor()
        cursor.execute(query, conn)
        conn.commit()
        cursor.close()
        print(f"Dados inseridos com sucesso na tabela {TargetTable}")
    
    except Exception as e:
        print(f"Ocorreu um erro: {e}")
    finally:
        conn.close()

# Exemplo de uso
Procedure = 'dbo.sp_GoldProcedure'
TargetTable = 'dbo.BeerGoldLayer'
InsertWithProcedure(Procedure, TargetTable)


