import os
import json
from pathlib import Path
from dotenv import load_dotenv


class ConfigurationManager:

    def __init__(self, city_name=""):
        load_dotenv(Path('configuration/.env'))
        self.city_name = city_name

    def get_listings_path(self, city_name):
        cities_data = json.load(open(os.getenv('CITIES_URL_DATA')))
        return cities_data[city_name]["listings_path"]

    def get_city_configuration(self, city_name):
        cities_data = json.load(open(os.getenv('CITIES_URL_DATA')))
        return cities_data[city_name]

    def get_city_url(self, city_name):
        cities_data = json.load(open(os.getenv('CITIES_URL_DATA')))
        return cities_data[city_name]["url"]

    def get_configured_cities_names(self):
        cities_data = json.load(open(os.getenv('CITIES_URL_DATA')))
        return cities_data.keys()
