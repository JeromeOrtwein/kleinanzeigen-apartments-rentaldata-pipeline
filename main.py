import scraper.kleinanzeigen_scraper as ks
from local_file_management import local_file_manager as lfm
from gcs_file_management import gcs_file_manager as gfm
from big_query_management import big_query_manager as bqm
from local_file_management.configuration_manager import ConfigurationManager
from prefect import flow

import random
import os
import argparse

waiting_seconds = 180


@flow(log_prints=True)
def validate_arguments(city_name):
    cm = ConfigurationManager()
    if city_name in cm.get_configured_cities_names():
        return True
    else:
        print(f"There is no city with name: {city_name} defined in the configuration")
        return False


@flow(log_prints=True)
def kleinanzeigen_to_local(city_name):
    cm = ConfigurationManager()
    print(f"Starting to request apartment rental data from ebay-kleinanzeigen for city: {city_name}")
    scraped_postings_list = ks.request_data_for_specified_city(city_name, cm.get_city_configuration(city_name))
    local_file_manager = lfm
    local_file_manager.add_new_listings_to_local_json_files(scraped_postings_list, cm.get_listings_path(city_name))
    df = local_file_manager.add_new_listings_to_local_parquet_files(scraped_postings_list,
                                                                    cm.get_listings_parquet_path_for_current_month(
                                                                        city_name))
    return df


@flow(log_prints=True)
def local_to_gcs(city_name):
    cm = ConfigurationManager()
    local_parquet_path = cm.get_listings_parquet_path_for_current_month(city_name)
    gcs_path = cm.get_gcs_path_for_city(city_name)
    gfm.copy_local_parquet_file_for_current_month(local_parquet_path, gcs_path)


@flow(log_prints=True)
def new_rows_to_bq(city_name, dataset_name, new_rows_df):
    bqm.update_data_for_current_listings(dataset_name, city_name, new_rows_df)


@flow(log_prints=True)
def kleinanzeigen_to_local_to_gcs_to_bq(city_name):
    cm = ConfigurationManager()
    dataset_name = cm.get_bq_dataset_name()
    current_listings_df = kleinanzeigen_to_local(city_name)
    local_to_gcs(city_name)

    # TODO: check for existing dataset on GCP BigQuery, this fails when the dataset doesn't exist.

    if not bqm.check_if_table_exists(dataset_name, city_name):
        bqm.create_big_query_table(dataset_name, city_name)
        if not bqm.check_if_table_exists(dataset_name, city_name):
            print("table could not get created successfully!")
            return
    new_rows_to_bq(city_name, dataset_name, current_listings_df)


@flow(log_prints=True)
def kleinanzeigen_main_flow(city_name):
    parser = argparse.ArgumentParser()
    parser.add_argument('--city_name',
                        help='The name of the city that should be used to scrape apartment'
                             ' rental data and ingest it to gcs')
    parser.add_argument('--initial_flag',
                        help='If True the first 100 pages get scraped from ebay-kleinanzeigen to get rental data else '
                             'only the first pages containing the today and yesterday identifier after that it stops')
    args = parser.parse_args()

    random.seed(os.getenv("PYTHONHASHSEED"))
    validate_arguments(city_name)
    kleinanzeigen_to_local_to_gcs_to_bq(city_name)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    default_city_name = "stuttgart"
    kleinanzeigen_main_flow(default_city_name)
