# City Rentals Pipeline
<hr/>

Final Project created for [Data Engineering Zoomcamp 2023](https://github.com/DataTalksClub/data-engineering-zoomcamp)
<hr/>

## Contents

- [Problem description](#problem-description)
- [Pipeline diagram and Pipeline description](#pipeline-description)
- [Dashboard](#dashboard)
- [Setup](#setup)

<hr/>

### Problem Description

This project gives a general overview about the apartment rental price trends and the current situation in the german city stuttgart.
It shows a highlighted map with the different boroughs and shows the discrepencies of rental prices based on those borough as well as the different post codes. Additionally the number of new lisings and the average Size based on the borough are shown in the dashboard. 

The datasource is "ebay-kleinanzeigen.de" which is a big platform for selling used products and posting appartment rental listings as well. The information that can be gathered using the overview page include:
- time stamp
- postcodes
- borough names 
- elipsis 
- rental price
- number of rooms
- size of apartment. 
<hr/>

### Pipeline Description
The pipeline consists of the following steps:

1. Prefect triggers daily
    1. Data Scraped from ebay-kleinanzeigen.de
        1. Basic Cleaning Process
        2. Creating Hash to avoid duplicates in data
        3. Transform data to parquet via Schema
        4. Save data locally (backup)
    2. Ingestion of data into Google Cloud Storage
        1. Update one File per day per city with new data
    3. Ingestion of newly found listings into Big Query
2. dbt triggers daily (But later)
    1. Ingestion of data into Google Big Query
        1. Raw Cleaned Data
        2. Aggregated Data (Fact Tables)
3. Dashboard using Tableau
    1. Tableau supports map data formats to visualize the cities depending on their postcodes. A geo location .kml file is used to create the map with the correct outlinings of the post code borders. 
    2. Line Chart it used to show the trend of the rental prices over time
    3. Bar Charts are used to show the number of listings based on the borough name and to show the average size of the appartment based on the borough name


A diagram showing the dataflow can be found here:

![Data Flow](https://user-images.githubusercontent.com/19490638/230110194-d2c9ca54-2087-49b7-bd76-4fc3ea481d12.png)
<hr/>

### Dashboard

The dashboard is stored in [Tableau Pubic](https://public.tableau.com/app/profile/jerome.ortwein/viz/StuttgartRentalDataVisualization/Dashboard1) - This wont be updated due to the restrictions of the zoomcamps final exam. I will deploy another dashboard with the live data that will be updated daily on the same tableau profile.

![Dashboard](https://user-images.githubusercontent.com/19490638/230111456-acbed137-c613-4926-9daf-2d1ef34b50ad.png)

As the pipeline is quiet new and scrapes the data once a day there is not a lot of data yet. Therefore the findings found in this dashboard are not representetive yet but will be over time. 

The dashboard in its default state shows on the left the districts of the city stuttgart in germany based on the postcodes. The Average Rental Prices per m² is written in the district borders with a according color. 
Filter options for this part of the dashboard are found on the top.
1. Finding: It will show what districts are priced higher than others. My prediction is that appartments around the city center will probably be the most expensive. But this remains to be verified.

On the top right side of the dashboard there is a trendline over time shown. The individual boroughs can be filtered out s.t. The trendline for individual boroughs or a aggregation of selected boroughs is possible.

2. Finding: It will show how the rental prices for appartments change over time. My prediction is that due to the trend that more people tend to live in big cities the rental prices could go up over time. But this remains to be verified. 

On the lower right side there are additional bar charts showing the number of listings per borough and the average listing size per borough. The same borough filter applies here as well. Additionally the bars are highlighted in the color of the average rental price per m². The data can be sorted by clicking on the charts axes title ('Number of Listings' or 'Listing Size in m²')

3. Finding: It will show what boroughs have the most new listings and where the biggest appartments are located. My prediction is that the bigger the borough is the more new listings are posted and that appartments near the city center will tend to have smaller sizes on average. But this remains to be verified. 

### Setup

The deployment was done on a locally hosted virtual machine (Running ubuntu). If a Infrastructure as a Service Virtual Machine is used this should work as well (If the scraping mechanism is not blocked for some reason)

1. Setup Credentials
- Google Cloud: Service-Accounts (Navigate IAM -> Service-Accounts -> Create Service-Account)
  - Rights to read/write to BigQuery for your Project
  - Rights to read/write to Google Cloud Storage for your Project
  - Example roles: BigQuery-Administrator, Storage-Administrator, Storage-Object-Administrator
2. Project Setup
- The project includes a requirements.txt file. That helps installing all the required dependencies (This installs prefect as well as the python libraries required)
    - pip install -r requirements.txt
- Create the needed blocks in prefect via ui or cli
    - GCSBucket Block: Name it `rental-data-bucket` and link it to a Credentials block with the according GCS Roles  using the service accounts json file
        - Data Backup will be written to gcs folder rentals-data/<city_name>
    - BigQueryWarehouse Block: Name it `big-query-block` and link it to a Credentials block with the according BQ Roles using the service-account json file
- Either use the provided 'kleinanzeigen_main_flow-deployment.yaml' file or run 
    `prefect deployment build ./main.py:kleinanzeigen_main_flow` and add 'city_name: stuttgart' to the parameters section
- Use the UI or CLI to schedule the flow to your liking
- Start the agent where you deployed the script by default its the default agent:
    - `prefect agent start --pool default-agent-pool --work-queue default`
3. dbt Setup
- Use the BigQuery Connector in the dbt project setup in dbt cloud
    - Use the same bigquerywarehouse credentials file as in 2. or alt. create a new service account in google cloud with the same rights
- All the needed models / schemas are placed in the dbt folder. 
    - You can just copy that into your dbt project 
- Schedule the run to your liking but try to have at least 10 minutes between the prefect and the dbt run

4. Dashboard Setup
- In Tableau you can use the BigQuery Connector feature and reference the service account again. (Only needed if you want to use the live data from big query -> some extracted data is already in this project)
- The files in the folder "tableau_data" include everything needed just open the .twb project using Tableau Public (You need a licence or the free trial)
- After connecting the datasource and extracting the data (or use the extracts provided in this git) use the menu "Server -> Tableau Public -> Save to Tableau Public" 

### Adapting for other cities

It is possible to adapt the script to scrape data for other cities as well.

1. Change the parameter in the kleinanzeigen_main_flow-deployment.yaml file from ('stuttgart' to ex. 'muenchen') to now scrape the data of munich. The url to the ebay-kleinanzeigen needs to be appended in the configuration/cities_url_data.json file. I already entered the url for 'stuttgart', 'muenchen', 'koeln', 'frankfurt-am-main'. 
2. The GCS path automatically changes accordingly to 'rentals-data/muenchen/muenchen_<year>_<month>.parquet'
3. The Big Query Table is named after the new city 'muenchen'
4. For dbt the according references to the bigquery table needs to be adapted manually for now. I hope to change that in the future. 


