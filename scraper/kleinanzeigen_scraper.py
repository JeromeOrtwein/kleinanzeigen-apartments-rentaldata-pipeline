import re
import requests
from requests.exceptions import HTTPError
from bs4 import BeautifulSoup
from datetime import date, datetime, timedelta
from hashlib import pbkdf2_hmac
from prefect import task, flow


@task(log_prints=True)
def get_rent_data_pages(base_url):
    """
    Uses the requests library to get the ebay-kleinanzeigen pages that contain the apartment rental data for a
    specific city.

    :param base_url: The base url to ebay-kleinanzeigen containing a placeholder for the page that should be scraped
    :return: Returns the strings of the individual ebay-kleinanzeigen pages containing apartment rental data
    for one specific city
    :rtype: str
    """
    not_last = True
    current_page = 1
    all_pages = []

    # only check the first 100 pages
    while not_last and current_page < 100:
        url = base_url % current_page
        try:
            response = requests.get(url, headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.80 Safari/537.36',
                'Accept-Encoding': 'identity', 'Content-Type': 'text/html; charset=utf-8'})
            response.raise_for_status()
        except HTTPError as http_err:
            print(f'HTTP error occurred: {http_err}')  # Python 3.6
        except Exception as err:
            print(f'Other error occurred: {err}')  # Python 3.6
        if int(re.search(r"seite:[0-9]+", str(response.url)).group(0).replace("seite:", "")) == current_page:
            # need to replace the zero width whitespace
            all_pages.append(str(response.content.decode('utf-8', 'strict')).replace("&#8203", ""))
            current_page += 1
        else:
            not_last = False
    return all_pages


# todo: document and create better filter mechanism
def filter_accept(title, price, qm_price) -> bool:
    '''
    Filters out certain unrealistic values and misplaced listings.

    :param title: title of listing dataset
    :param price:   price of listing dataset
    :param qm_price: price in m² of listing dataset
    :return: returns if a city dataset is accepted as valid
    '''
    # for bigger cities in germany realistic filters, may not apply to smaller cities
    return "such" not in title.lower() and not qm_price > 40 and not qm_price < 5


@task(log_prints=True)
def clean_rent_data(posting):
    """
    Cleans certain aspects of an apartment rental listings and creates a dictionary for each listing

    :param posting: The BeautifulSoup representation of the webelement containing the posting
    :type posting: BeautifulSoup Tag
    :return: dictionary containing the relevant data of a rental listing
    """
    plz = posting.find_all("div", class_="aditem-main--top--left")[0].text.strip().split(" ")[0].replace(" ", "")
    borough = posting.find_all("div", class_="aditem-main--top--left")[0].text.strip().split(" ", 1)[1]
    # Time of posting possibilities (Heute, Gestern, DD.MM.YYYY, <none>) : Heute = today, Gestern = yesterday
    time_of_posting_string = posting.find_all("div", class_="aditem-main--top--right")[0].text.replace(" ", "")
    pattern = r"\d{2}\.\d{2}\.\d{4}"
    match = re.search(pattern, time_of_posting_string)
    if match:
        posting_date = datetime.strptime(match.group(), "%d.%m.%Y").date()
    elif "Heute" in time_of_posting_string:
        posting_date = date.today()
    elif "Gestern" in time_of_posting_string:
        posting_date = date.today() - timedelta(days=1)
    else:
        posting_date = date.today()

    posting_ellipsis = posting.find_all("a", class_="ellipsis")[0].text.strip()
    posting_price = posting.find_all("p", class_="aditem-main--middle--price-shipping--price")[0].text.replace("\n",
                                                                                                               "").replace(
        " ", "").replace("VB", "").replace("€", "").replace(".", "").replace(",", ".")
    posting_qm = posting.find_all("span", class_="simpletag")[0].text.replace(" m²", "").replace(
        ",",
        ".")

    posting_room_count = posting.find_all("span", class_="simpletag")[1].text.replace(
        "Zimmer", "").replace(" ", "").replace(",", ".")

    price_per_qm = round(float(posting_price) / float(posting_qm), 2)
    # HASH ON COMPLETE DATASET TO AVOID DUPLICATES / Reposts -> set in Database to unique or filter them beforehand
    string_to_hash = plz + borough + posting_ellipsis + str(posting_price) + str(posting_qm) + str(
        posting_room_count)
    current_hash = pbkdf2_hmac('sha256', bytes(string_to_hash, "utf-8"), b'07121993', 1)

    posting_entry = {}
    if filter_accept(posting_ellipsis, posting_price, price_per_qm):
        posting_entry = {"hash_code": str(current_hash),
                         "post_code": plz,
                         "borough": borough,
                         "title": posting_ellipsis,
                         "price": float(posting_price),
                         "posting_qm": float(posting_qm),
                         "posting_room_count": float(posting_room_count),
                         "price_per_qm": float(price_per_qm),
                         "time_stamp": str(posting_date)}

    return current_hash, posting_entry


@flow(log_prints=True)
def convert_rent_data_page_to_dictionary(page_strings):
    """
    Identifies the individual listings containing rental data and returns a list of dictionaries containing all
    listings found in the page_strings.

    :param page_strings: The strings of the ebay-kleinanzeigen website containing the relevant elements
    :type page_strings: str
    :return: Returns the dictionary containing all the postings found in the ebay-kleinanzeigen website
    :rtype: dict
    """
    all_posting_list = []
    for page_string in page_strings:
        soup = BeautifulSoup(page_string, 'html.parser')
        all_postings = soup.find_all("div", class_="aditem-main")
        for posting in all_postings:
            try:
                current_hash, posting_entry = clean_rent_data(posting)
                # add to all posting list
                if any(posting_entry):
                    all_posting_list.append(posting_entry)
            except IndexError:
                continue
            except ValueError:
                continue
            except ZeroDivisionError:
                continue

    return all_posting_list


def request_data_for_specified_city(city_name, city_configuration):
    print(f"Requesting rental data for {city_name}!")
    print("please wait...")
    all_rent_data_pages = get_rent_data_pages(city_configuration["url"])
    print(f"Finished requesting for {city_name}")

    all_posting_dict_list = convert_rent_data_page_to_dictionary(all_rent_data_pages)

    print(all_posting_dict_list)
    return all_posting_dict_list
