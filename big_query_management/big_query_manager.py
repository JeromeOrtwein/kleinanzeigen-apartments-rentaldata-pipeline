from prefect_gcp import GcpCredentials
from prefect_gcp.bigquery import BigQueryWarehouse

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
CREATE TABLE IF NOT EXISTS `%s.%s` (
    hash_code STRING,
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
@task(log_prints=True)
def update_data_for_current_listings(df, city_name, project_name):
    """Write DataFrame to BigQuery"""
    gcp_credentials_block = GcpCredentials.load("gcp-rental-data-credentials")
    df.to_gbq(destination_table=f"{city_name}_rental_data",
              project_id="striped-harbor-375816",
              credentials=gcp_credentials_block.get_credentials_from_service_account(),
              chunksize=500_000,
              if_exists="append")
    bigquery_warehouse_block = BigQueryWarehouse.load("big-query-block")


@task(log_prints=True)
def check_if_table_exists(city_name):
    """Checks if the table for a specified city already exists in big query"""

    print("Checking if table exists")


def create_big_query_table(city_name):
    """Creates a new table in big query"""
    print("Creating new table in big query for the given city")
