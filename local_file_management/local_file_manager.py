import json


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
                                    "time_stamp": str}

    def add_new_listings_to_local_files(self, all_postings_list, city_json_file):
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
