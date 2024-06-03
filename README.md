# TestCaseBeer

This repository contains a case study on a brewery database by location, where you can examine the entire data engineering workflow. The following tools were used:
- PDI (Pentaho Data Integration) for orchestration;
- Python for API requests and data processing across the three layers (Bronze, Silver, and Gold).

## Scripts Overview

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