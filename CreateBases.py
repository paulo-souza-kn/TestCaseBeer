import pyodbc
import pandas as pd

#Create tables

#Step 2:Create SQL connection variables
server = 'BeersServerTest'
username = 'BeerUser'
password = 'BeerUser'
driver = '{ODBC Driver 17 for SQL Server}'

# Step 3: Função para criar tabela se não existir
def CreateTableIfNotExists(database,TableName, Schema):

    try:
        # Estabelece a conexão
        database = database
        
        # Cria a string de conexão
        connection_string = f'DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password}'
        
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        # Verifica se a tabela existe
        check_table_query = f"""
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{TableName}')
        BEGIN
            {Schema}
        END
        """

        # Executa o comando
        cursor.execute(check_table_query)
        conn.commit()
        cursor.close()
        print(f"Tabela '{TableName}' verificada/criada com sucesso.")
    
    except Exception as e:
        print(f"Ocorreu um erro: {e}")
    
    finally:
        conn.close()

        
# Create Bronze table
database = 'db_bronze'
TableName = 'dbo.BeerBronzeLayer'
Schema = """
CREATE TABLE dbo.BeerBronzeLayer (
    IdRow INT PRIMARY KEY,
    Name NVARCHAR(255),
    BreweryType NVARCHAR(255),
    AddressOne NVARCHAR(255),
    AddressTwo NVARCHAR(255),
    AddressThree NVARCHAR(255),
    City NVARCHAR(255),
    StateProvince NVARCHAR(255),
    PostalCode NVARCHAR(255),
    Country NVARCHAR(255),
    Longitude NVARCHAR(255),
    Latitude NVARCHAR(255),
    Phone NVARCHAR(255),
    WebSiteURL NVARCHAR(255),
    State NVARCHAR(255),
    Street NVARCHAR(255),
    LineHash NVARCHAR(255),
    FileName NVARCHAR(255),
    BlockHash NVARCHAR(255),
    InsertDate DATETIME
)
"""
CreateTableIfNotExists(TableName, Schema)

# Create Silver table
database = 'db_silver'
TableName = 'BeerSilverLayer'
Schema = """
CREATE TABLE dbo.BeerSilverLayer (
    IdRow INT PRIMARY KEY,
    Name NVARCHAR(255),
    BreweryType NVARCHAR(255),
    AddressOne NVARCHAR(255),
    AddressTwo NVARCHAR(255),
    AddressThree NVARCHAR(255),
    City NVARCHAR(255),
    StateProvince NVARCHAR(255),
    PostalCode NVARCHAR(255),
    Country NVARCHAR(255),
    Longitude NVARCHAR(255),
    Latitude NVARCHAR(255),
    Phone NVARCHAR(255),
    WebSiteURL NVARCHAR(255),
    State NVARCHAR(255),
    Street NVARCHAR(255),
    LineHash NVARCHAR(255),
    BlockHash NVARCHAR(255),
    InsertDate DATETIME
)
"""
CreateTableIfNotExists(TableName, Schema)

# Create Gold table
database = 'db_gold'
TableName = 'dbo.BeerGoldLayer'
Schema = """
CREATE TABLE dbo.BeerGoldLayer (
    IdRow INT PRIMARY KEY,
    Name NVARCHAR(255),
    BreweryType NVARCHAR(255),
    AddressOne NVARCHAR(255),
    City NVARCHAR(255),
    State NVARCHAR(255),
    Street NVARCHAR(255),
    PostalCode NVARCHAR(255),
    Country NVARCHAR(255),
    Longitude NVARCHAR(255),
    Latitude NVARCHAR(255),
    Phone NVARCHAR(255),
    WebSiteURL NVARCHAR(255),
    LineHash NVARCHAR(255),
    BlockHash NVARCHAR(255),
    InsertDate DATETIME
)
"""
CreateTableIfNotExists(TableName, Schema)

def ExecuteSilverProcedure(Procedure, TargetTable):

    try:
        
        #Step 2:Create SQL connection variables
        server = 'BeersServerTest'
        database = 'db_silver'
        username = 'BeerUser'
        password = 'BeerUser'
        driver = '{ODBC Driver 17 for SQL Server}'

        # Cria a string de conexão
        connection_string = f'DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password}'

        # Estabelece a conexão
        conn = pyodbc.connect(connection_string)
        
        # Ler a tabela de origem
        query = f"""
        CREATE {Procedure} AS 
            BEGIN 
                    DECLARE @InsertDatas TABLE (
                    BronzeLineHash VARCHAR(255),
                    SilverLineHash VARCHAR(255)
                );

                -- Preenchimento da tabela @InsertDatas com os resultados da consulta
                INSERT INTO @InsertDatas (BronzeLineHash, SilverLineHash)
                SELECT DISTINCT
                    Bronze.LineHash AS BronzeLineHash,
                    Silver.LineHash AS SilverLineHash
                FROM
                    db_bronze.dbo.BeerBronzeLayer AS Bronze
                LEFT JOIN
                    db_silver.dbo.BeerSilverLayer AS Silver
                ON
                    Bronze.LineHash = Silver.LineHash
                WHERE
                    Silver.LineHash IS NULL

                ;WITH s1 AS (
                    SELECT
                        Name,
                        BreweryType,
                        AddressOne,
                        AddressTwo,
                        AddressThree,
                        City,
                        StateProvince,
                        PostalCode,
                        Country,
                        Longitude,
                        Latitude,
                        Phone,
                        WebSiteURL,
                        State,
                        Street,
                        LineHash,
                        BlockHash,
                        CONVERT(DATETIME,InsertDate) AS InsertDate
                    FROM db_bronze.dbo.BeerBronzeLayer Bronze
                    LEFT JOIN
                    (
                        SELECT DISTINCT
                            BronzeLineHash
                        FROM
                            @InsertDatas
                    ) AS InsertDatas
                    ON
                        Bronze.LineHash = InsertDatas.BronzeLineHash
                    WHERE InsertDatas.BronzeLineHash IS NOT NULL
                )
                INSERT INTO db_silver.dbo.BeerSilverLayer (
                    -- Colunas
                        Name,
                        BreweryType,
                        AddressOne,
                        AddressTwo,
                        AddressThree,
                        City,
                        StateProvince,
                        PostalCode,
                        Country,
                        Longitude,
                        Latitude,
                        Phone,
                        WebSiteURL,
                        State,
                        Street,
                        LineHash,
                        BlockHash,
                        InsertDate
                )
                SELECT
                    -- Colunas
                        Name,
                        BreweryType,
                        AddressOne,
                        AddressTwo,
                        AddressThree,
                        City,
                        StateProvince,
                        PostalCode,
                        Country,
                        Longitude,
                        Latitude,
                        Phone,
                        WebSiteURL,
                        State,
                        Street,
                        LineHash,
                        BlockHash,
                        InsertDate
                FROM s1;

                WITH CTE AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (PARTITION BY LineHash ORDER BY InsertDate DESC)AS rn
                FROM
                    db_silver.dbo.BeerSilverLayer WITH (NOLOCK))
                DELETE
                FROM 
                    CTE
                WHERE rn > 1;
                
                UPDATE db_silver.dbo.BeerSilverLayer
                SET 
                    AddressOne = ISNULL(AddressOne, 'Others'),
                    AddressTwo = ISNULL(AddressTwo, 'Others'),
                    AddressThree = ISNULL(AddressThree, 'Others'),
                    Longitude = ISNULL(Longitude, '0'),
                    Latitude = ISNULL(Latitude, '0'),
                    Phone = ISNULL(Phone, 'Others'),
                    website_url = ISNULL(website_url, 'WithoutWebSite')
                WHERE 
                    AddressOne IS NULL OR
                    AddressTwo IS NULL OR
                    AddressThree IS NULL OR
                    Longitude IS NULL OR
                    Latitude IS NULL OR
                    Phone IS NULL OR
                    website_url IS NULL;
                
            END
        """
        # Inserir os dados transformados na tabela de destino
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        cursor.close()
        print(f"Procedure criada para a tabela {TargetTable}")
    
    except Exception as e:
        print(f"Ocorreu um erro: {e}")
    finally:
        conn.close()
Procedure = 'dbo.sp_SilverProcedure',
TargetTable = 'BeerSilverLayer'
ExecuteSilverProcedure(Procedure, TargetTable)
        

def ExecuteGoldProcedure(Procedure, TargetTable):

    try:
        
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
        
        # Ler a tabela de origem
        query = f"""
        CREATE {Procedure} AS 
            BEGIN 
                    DECLARE @InsertDatas TABLE (
                    SilverLineHash VARCHAR(255),
                    GoldLineHash VARCHAR(255)
                );

                -- Preenchimento da tabela @InsertDatas com os resultados da consulta
                INSERT INTO @InsertDatas (SilverLineHash, GoldLineHash)
                SELECT DISTINCT
                    Silver.LineHash AS BronzeLineHash,
                    Gold.LineHash AS SilverLineHash
                FROM
                    db_silver.dbo.BeerSilverLayer AS Silver
                LEFT JOIN
                    db_gold.dbo.BeerGoldLayer AS Gold
                ON
                    Silver.LineHash = Gold.LineHash
                WHERE
                    Gold.LineHash IS NULL

                ;WITH s1 AS (
                    SELECT
                        Name,
                        BreweryType,
                        AddressOne,
                        AddressTwo,
                        AddressThree,
                        City,
                        StateProvince,
                        PostalCode,
                        Country,
                        Longitude,
                        Latitude,
                        Phone,
                        WebSiteURL,
                        State,
                        Street,
                        LineHash,
                        BlockHash,
                        InsertDate
                    FROM db_silver.dbo.BeerSilverLayer Silver
                    LEFT JOIN
                    (
                        SELECT DISTINCT
                            SilverLineHash
                        FROM
                            @InsertDatas
                    ) AS InsertDatas
                    ON
                        Silver.LineHash = InsertDatas.SilverLineHash
                    WHERE InsertDatas.SilverLineHash IS NOT NULL
                )
                INSERT INTO db_gold.dbo.BeerSilverLayer (
                    -- Colunas
                        Name,
                        BreweryType,
                        AddressOne,
                        City,
                        State,
                        Street,
                        PostalCode,
                        Country,
                        Longitude,
                        Latitude,
                        Phone,
                        WebSiteURL,
                        LineHash,
                        BlockHash,
                        InsertDate
                )
                SELECT
                    -- Colunas
                        Name,
                        BreweryType,
                        AddressOne,
                        City,
                        State,
                        Street,
                        PostalCode,
                        Country,
                        Longitude,
                        Latitude,
                        Phone,
                        WebSiteURL,
                        LineHash,
                        BlockHash,
                        InsertDate
                FROM s1;

                WITH CTE AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (PARTITION BY LineHash ORDER BY InsertDate DESC)AS rn
                FROM
                    db_gold.dbo.BeerGoldLayer WITH (NOLOCK))
                DELETE
                FROM 
                    CTE
                WHERE rn > 1;
            END
        """
        # Inserir os dados transformados na tabela de destino
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        cursor.close()
        print(f"Procedure criada para a tabela {TargetTable}")
    
    except Exception as e:
        print(f"Ocorreu um erro: {e}")
    finally:
        conn.close()
Procedure = 'dbo.sp_SilverProcedure',
TargetTable = 'BeerGoldLayer'
ExecuteSilverProcedure(Procedure, TargetTable)

