from prefect import task
from prefect_gcp.cloud_storage import GcsBucket
from pathlib import Path

@task(log_prints=True)
def copy_local_parquet_file_for_current_month(local_file_path, gcs_file_path):
    gcs_bucket_block = GcsBucket.load("rental-data-bucket")
    print("copying local file to gcs")
    gcs_path_with_file = gcs_file_path + Path(local_file_path).name
    gcs_bucket_block.upload_from_path(local_file_path, gcs_path_with_file)
