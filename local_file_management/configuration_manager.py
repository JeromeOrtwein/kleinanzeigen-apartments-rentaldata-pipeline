import os
import json
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime


class ConfigurationManager:

    def __init__(self, city_name=""):
        load_dotenv(Path('configuration/.env'))
        self.city_name = city_name

    def get_listings_path(self, city_name):
        """
        Returns the path to the listings json file.

        :param city_name: The name of the city
        :return: Path of the listings json file
        """
        cities_data = json.load(open(os.getenv('CITIES_URL_DATA')))
        return cities_data[city_name]["listings_path"]

    def get_city_configuration(self, city_name):
        """
        Returns the configuration for a specific city.

        :param city_name: The name of the city
        :return: Configuration of the city
        """
        cities_data = json.load(open(os.getenv('CITIES_URL_DATA')))
        return cities_data[city_name]

    def get_city_url(self, city_name):
        """
        Returns the url to ebay-kleinanzeigen.de where the rental data listings are located.

        The urls have to be defined in the configuration file configuration/cities_url_data.json
        :param city_name: The name of the city
        :return: The url as string
        """
        cities_data = json.load(open(os.getenv('CITIES_URL_DATA')))
        return cities_data[city_name]["url"]

    def get_configured_cities_names(self):
        """
        Returns the list of all the cities that are configured in the configuration file.

        configuration file located at /configuration/cities_url_data.json
        :return: The name of the cities that are configured
        """
        cities_data = json.load(open(os.getenv('CITIES_URL_DATA')))
        return cities_data.keys()

    def get_listings_parquet_path_for_current_month(self, city_name):
        """
        Returns the parquet file location for the city and current year/month.

        The file not necessarily exists yet
        :param city_name: The name of the city
        :return: The path to the parquet file
        """
        listings_path = self.get_listings_path(city_name)
        year_month = datetime.now().strftime("_%Y_%m")
        listings_dir = Path(listings_path).parent / (city_name + year_month)
        listings_dir_string = listings_dir.as_posix() + ".parquet"
        return listings_dir_string
