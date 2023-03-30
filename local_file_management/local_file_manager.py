import json
import pandas as pd
import os.path
import pyarrow.parquet as pq


class LocalFileManager:
    def __init__(self, city_name):
        self.city_name = city_name
        self.rental_entry_schema = {"hash": str,
                                    "post_code": str,
                                    "borough": str,
                                    "title": str,
                                    "price": float,
                                    "posting_qm": float,
                                    "posting_room_count": float,
                                    "price_per_qm": float,
                                    "time_stamp": 'datetime64'}

    def add_new_listings_to_local_json_files(self, all_postings_list, city_json_file):
        """
        Adds new listings to a json file containing all listings for a specific city.

        :param all_postings_list: All postings as list containing dictionaries
        :param city_json_file: The path to the json file that the listings should be read/written to
        :return: None
        """
        try:
            with open(city_json_file, 'r') as all_listings_file:
                all_listings_in_file = json.load(all_listings_file)

            all_hashes_in_listings = [listing["hash"] for listing in all_listings_in_file]
            # with open(hash_file, 'r') as all_listings_file_hashes:
            # all_hashes_in_file = json.load(all_listings_file_hashes)

            hashes_in_this_run = []
            new_listings_in_this_run = []
            for idx, posting in enumerate(all_postings_list):
                if posting["hash"] in all_hashes_in_listings or posting["hash"] in hashes_in_this_run:
                    continue
                hashes_in_this_run.append(posting["hash"])
                all_listings_in_file.append(posting)
                new_listings_in_this_run.append(posting)

            with open(city_json_file, 'w') as json_file:
                # print(all_listings_in_file)
                json.dump(all_listings_in_file, json_file, indent=4)
            return

        except FileNotFoundError:
            print("File Not Found")

    def add_new_listings_to_local_parquet_files(self, all_postings_list, city_parquet_file_path):
        """
        Adds new listings to the existing local parquet file or creates a new parquet file for this month.

        :param all_postings_list: List of all the postings as dictionary
        :param city_parquet_file_path: The path to where the parquetfile for this month shoud be read/written
        :return: None
        """
        # One parquetfile per month
        rental_df = pd.DataFrame(all_postings_list)
        rental_df = rental_df.astype(self.rental_entry_schema)
        if not os.path.exists(city_parquet_file_path):
            rental_df.to_parquet(f'{city_parquet_file_path}')
        else:
            existing_df = pd.read_parquet(f'{city_parquet_file_path}')
            merged = existing_df.merge(rental_df, on="hash", how='outer', indicator=True)
            # Creates a dataframe with the new rows
            try:
                combined_dataframe = merged.drop_duplicates(subset=["hash"])
                combined_dataframe.to_parquet(f'{city_parquet_file_path}')
            except KeyError as ke:
                print("No new listings!")
                return
