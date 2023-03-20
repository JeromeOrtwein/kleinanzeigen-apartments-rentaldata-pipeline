<h1>City Rentals Pipeline</h1>
This project gives a general overview about the apartment rental price trends in a selection of german cities over time.
Additionally it shows a highlighted map with the different boroughs and shows the discrepencies of rental prices.
The datasource is "ebay-kleinanzeigen.de". People can post the apartments they want to rent with additional information there.
This information includes postcodes, borough names, elipsis, rental price, number of rooms, size of apartment. 


The pipeline consists of the following steps:
1. Prefect triggers daily
   1. Data Scraped from ebay-kleinanzeigen.de
      1. Basic Cleaning Process
      2. Creating Hash to avoid duplicates in data
      3. Transform data to parquet via Schema
      4. Save data locally (backup)
   2. (Ingestion of data into Google Cloud Storage)
      1. Copy one File per day per city with new data
2. dbt triggers daily (but later)
   1. Ingestion of data into Google Big Query
      1. Raw Cleaned Data
      2. Aggregated Data (Fact Tables)
3. Dashboard (Tableau or Looker)
    1. Tableau supports map data formats to visualize the cities depending on their postcodes
    2. Maybe create a separate report to show data over time, trends in looker

A diagram showing the dataflow can be found here:

[Data Flow](https://lucid.app/lucidspark/fe313eda-790d-4a6d-9fd8-9b9b2950f73a/edit?viewport_loc=-28%2C-447%2C2106%2C2160%2C0_0&invitationId=inv_67a7b962-ef9f-449f-875c-27238946a77c)


This pipeline was built as a project for the data engineering zoomcamp by DataTalksClub 

[Zoomcamp Github](https://github.com/DataTalksClub/data-engineering-zoomcamp)


