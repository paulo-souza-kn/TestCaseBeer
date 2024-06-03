# TestCaseBeer

This repository contains a case study on a brewery database by location, where you can examine the entire data engineering workflow. The following tools were used:
- PDI (Pentaho Data Integration) for orchestration;
- Python for API requests and data processing across the three layers (Bronze, Silver, and Gold).

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
This Scripts is the second interable with the database, where insert raw data into bronze layer