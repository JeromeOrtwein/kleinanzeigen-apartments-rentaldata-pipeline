from prefect_gcp import GcpCredentials
from prefect_gcp.bigquery import BigQueryWarehouse
from prefect import task

rental_entry_schema = {"hash_code": str,
                       "post_code": str,
                       "borough": str,
                       "title": str,
                       "price": float,
                       "posting_qm": float,
                       "posting_room_count": float,
                       "price_per_qm": float,
                       "time_stamp": 'datetime64'}

sql_create_table = """ 
CREATE TABLE IF NOT EXISTS `{}.{}` (
    hash_code STRING NOT NULL,
    post_code STRING,
    borough STRING,
    title STRING,
    price FLOAT64,
    posting_qm FLOAT64,
    posting_room_count FLOAT64,
    price_per_qm FLOAT64,
    time_stamp DATE
)
"""

sql_check_table_existence = """
SELECT COUNT(*)
FROM `{}.INFORMATION_SCHEMA.TABLES`
WHERE table_name = '{}'
"""

sql_check_hash_in_table = """
SELECT COUNT(hash_code)
FROM (
    SELECT "{}" AS hash_code
)
WHERE hash_code NOT IN (
    SELECT hash_code FROM {}.{}
)
"""


@task(log_prints=True)
def update_data_for_current_listings(dataset_name, city_name, df):
    """Write DataFrame to BigQuery"""
    bigquery_warehouse = BigQueryWarehouse.load("big-query-block")
    hash_column = df.loc[:, "hash_code"]
    list_of_new_hashes = []
    # Maybe it's faster to bundle those requests, but it seems to be more costly to unionize them in bigquery
    # Maybe generating a temporary table is the way
    # for now iterating must suffice as the dataframe should not get to big max 100 pages * 30 listings = 3000 rows
    print(df)
    for hash_value in hash_column:
        result = bigquery_warehouse.fetch_one(sql_check_hash_in_table.format(hash_value, dataset_name, city_name))
        if result and int(result[0]) > 0:
            list_of_new_hashes.append(hash_value)

    if len(list_of_new_hashes) == 0:
        print(f"No new listings found to append to big query for city {city_name}!")
        return

    # Filter the dataframe s.t. only rows with hashes that do not exist in big query are included
    new_listings = df[df["hash_code"].isin(list_of_new_hashes)]
    print(f'There are {len(new_listings)} new listings for city {city_name}')
    gcp_credentials_block = GcpCredentials.load("gcp-rental-data-credentials")
    new_listings.to_gbq(destination_table=f'{dataset_name}.{city_name}',
                        project_id=gcp_credentials_block.project,
                        credentials=gcp_credentials_block.get_credentials_from_service_account(),
                        if_exists="append")


@task(log_prints=True)
def check_if_table_exists(dataset_name, city_name):
    """Checks if the table for a specified city already exists in big query"""
    print("Checking if table exists")
    bigquery_warehouse = BigQueryWarehouse.load("big-query-block")
    table_exist_count = bigquery_warehouse.fetch_one(sql_check_table_existence.format(dataset_name, city_name))
    if table_exist_count[0] == 0:
        print(table_exist_count)
        return False
    else:
        print(table_exist_count)
        return True


@task(log_prints=True)
def create_big_query_table(dataset_name, city_name):
    """Creates a new table in big query"""
    print("Creating new table in big query for the given city")
    bigquery_warehouse = BigQueryWarehouse.load("big-query-block")
    bigquery_warehouse.execute(sql_create_table.format(dataset_name, city_name))
