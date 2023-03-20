from scraper.kleinanzeigen_scraper import KleinanzeigenScraper
from local_file_management.local_file_manager import LocalFileManager
from local_file_management.configuration_manager import ConfigurationManager
from prefect import flow, task

import random
import os
import time
import argparse

waiting_seconds = 180


# @flow(log_prints=True)
def validate_arguments(args):
    cm = ConfigurationManager()
    if args.city_name in cm.get_configured_cities_names():
        return True
    else:
        print(f"There is no city with name: {args.city_name} defined in the configuration")
        return False


# @flow(log_prints=True)
def kleinanzeigen_to_local(args):
    ks = KleinanzeigenScraper()
    cm = ConfigurationManager()
    city_name = args.city_name
    print(f"Starting to request apartment rental data from ebay-kleinanzeigen for city: {city_name}")
    scraped_postings_list = ks.request_data_for_specified_city(city_name, cm.get_city_configuration(city_name))
    local_file_manager = LocalFileManager(city_name)
    local_file_manager.add_new_listings_to_local_files(scraped_postings_list, cm.get_listings_path(city_name))


# @flow(log_prints=True)
def kleinanzeigen_to_local_to_gcs(args):
    kleinanzeigen_to_local(args)
    # Seems like they block to many requests in a short time
    # print(f"waiting for {waiting_seconds} seconds! To not spam servers!")
    # duplicateRemover.remove_duplicates_from_listings(city_data)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--city_name',
                        help='The name of the city that should be used to scrape apartment'
                             ' rental data and ingest it to gcs')
    parser.add_argument('--initial_flag',
                        help='If True the first 100 pages get scraped from ebay-kleinanzeigen to get rental data else '
                             'only the first pages containing the today and yesterday identifier after that it stops')
    args = parser.parse_args()

    random.seed(os.getenv("PYTHONHASHSEED"))
    validate_arguments(args)
    kleinanzeigen_to_local_to_gcs(args)

    # See PyCharm help at https://www.jetbrains.com/help/pycharm/
