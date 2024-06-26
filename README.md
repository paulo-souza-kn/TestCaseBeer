# TestCaseBeer
- Creator: Souza Paulo
- Creation date: 2024-05-29

## Summary
- [Introduction](#introduction)
- [Job Overview](#job-overview)
- [Scripts Overview](#scripts-overview)


## Introduction
This repository contains a case study on a brewery database by location, where you can examine the entire data engineering workflow. The following tools were used:
- PDI (Pentaho Data Integration) for orchestration;
- Python for API requests and data processing across the three layers (Bronze, Silver, and Gold);
- SQL language for create tables, procedures, inserts and views.

Desejable tools:
- Airflow
- Pyspark
- DataBricks
- Azure Cloud or any cloud
- Docker

## Job Overview
![alt text](image.png)

This job was create with PDI (Pentaho Data Integration) and works:
- Firts the job start the API request python script that get the raw data into API;
    - Condition 1: Try again if have any temporary error and if successful, then continue, else enter into condition 2;
    - Condition 2: Send an e-mail with a error log.
- Second the job start the insert data into databases with bronze layer python script that get the raw data and insert into bronze layer;
    - Condition 1: Try again if have any temporary error and if successful, then continue, else enter into condition 2;
    - Condition 2: Send an e-mail with a error log.
- Third the job start the Silver layer python script that convert data columns with the right data type;
    - Condition 1: Try again if have any temporary error and if successful, then continue, else enter into condition 2;
    - Condition 2: Send an e-mail with a error log.
- Fourth the job start the Gold layer python script that create an analyticials datas and insert into gold layer;
    - Condition 1: Try again if have any temporary error and if successful, then continue, else enter into condition 2;
    - Condition 2: Send an e-mail with a error log.
- Then, the last step is send an e-mail telling that the job was successful.

## Scripts Overview

### CreateBases.py
This Script have the table creation queries and procedures like:
- dbo.BeerBronzeLayer into db_bronze (Bronze layer for raw data);
- dbo.BeerSilverLayer into db_silver (Silver layer for raw data);
- dbo.BeerGoldLayer into db_gold (Gold layer for raw data).

The server structure is like this:
#### BeersServerTest
- **db_bronze**
  - **dbo.BeerBronzeLayer**: This is the initial raw data layer where unprocessed data is stored.
    - IdRow INT PRIMARY KEY
    - Name NVARCHAR(255),
    - BreweryType NVARCHAR(255),
    - AddressOne NVARCHAR(255),
    - AddressTwo NVARCHAR(255),
    - AddressThree NVARCHAR(255),
    - City NVARCHAR(255),
    - StateProvince NVARCHAR(255),
    - PostalCode NVARCHAR(255),
    - Country NVARCHAR(255),
    - Longitude NVARCHAR(255),
    - Latitude NVARCHAR(255),
    - Phone NVARCHAR(255),
    - WebSiteURL NVARCHAR(255),
    - State NVARCHAR(255),
    - Street NVARCHAR(255),
    - LineHash NVARCHAR(255),
    - FileName NVARCHAR(255),
    - BlockHash NVARCHAR(255),
    - InsertDate DATETIME

- **db_silver**
  - **dbo.BeerSilverLayer**: This layer contains data that has been cleaned and transformed for further processing.
    - **dbo.BeerBronzeLayer**: This is the initial raw data layer where unprocessed data is stored.
    - IdRow INT PRIMARY KEY,
    - Name NVARCHAR(255),
    - BreweryType NVARCHAR(255),
    - AddressOne NVARCHAR(255),
    - AddressTwo NVARCHAR(255),
    - AddressThree NVARCHAR(255),
    - City NVARCHAR(255),
    - StateProvince NVARCHAR(255),
    - PostalCode NVARCHAR(255),
    - Country NVARCHAR(255),
    - Longitude NVARCHAR(255),
    - Latitude NVARCHAR(255),
    - Phone NVARCHAR(255),
    - WebSiteURL NVARCHAR(255),
    - State NVARCHAR(255),
    - Street NVARCHAR(255),
    - LineHash NVARCHAR(255),
    - BlockHash NVARCHAR(255),
    - InsertDate DATETIME

- **db_gold**
  - **dbo.BeerGoldLayer**: This final layer holds the refined, high-quality data ready for analysis and reporting.
    - IdRow INT PRIMARY KEY,
    - Name NVARCHAR(255),
    - BreweryType NVARCHAR(255),
    - AddressOne NVARCHAR(255),
    - City NVARCHAR(255),
    - State NVARCHAR(255),
    - Street NVARCHAR(255),
    - PostalCode NVARCHAR(255),
    - Country NVARCHAR(255),
    - Longitude NVARCHAR(255),
    - Latitude NVARCHAR(255),
    - Phone NVARCHAR(255),
    - WebSiteURL NVARCHAR(255),
    - LineHash NVARCHAR(255),
    - BlockHash NVARCHAR(255),
    - InsertDate DATETIME


### API_Request.py
This scripts contain the API request <https://api.openbrewerydb.org/breweries> and get these columns: 
- id: Line unique identifier
- name: Beer name
- brewery_type: Type of brewery (e.g., micro, nano, regional)
- address_1: Primary street address of the brewery
- address_2: Secondary street address (if available)
- address_3: Tertiary street address (if available)
- city: City where the brewery is located
- state_province: State or province where the brewery is located
- postal_code: Postal code of the brewery's address
- country: Country where the brewery is located
- longitude: Longitude coordinate of the brewery's location
- latitude: Latitude coordinate of the brewery's location
- phone: Contact phone number for the brewery
- website_url: Website URL of the brewery
- state: State where the brewery is located
- street: Street address of the brewery (often similar to address_1)

### BronzeLayer.py
This Scripts is the second interable with the database, where insert raw data into bronze layer. Here have three main functions to insert:
- GetMostRecentFile(folder_path)
    - Params: folder_path (file location).
- CreateBronzeLayerDate(most_recent_file)
    - Params: most_recent_file (file location generated by function above).
- InsertIntoBronzeLayer(data, TableName)
    - Params: data (dataframe generated by most_recent_file);
    - TableName: (table into database that we wanna insert the data).

### SilverLayer.py
This Scripts is the third interable with the database, where insert convertional data into silver layer. Here have only one main functions to insert:
- InsertWithProcedure(Procedure, TargetTable)
    - Params: Procedure (Stored Procedured that are in CreateBases.py for insert convertional data);
    - TargetTable: (What data table will go the convertional data).


### GoldLayer.py
This Scripts is the fourth interable with the database, where insert analytical data into gold layer. Here have only one main functions to insert:
- InsertWithProcedure(Procedure, TargetTable)
    - Params: Procedure (Stored Procedured that are in CreateBases.py for insert analytical data);
    - TargetTable: (What data table will go the convertional data).