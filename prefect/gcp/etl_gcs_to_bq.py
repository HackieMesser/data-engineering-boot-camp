from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(retries=3)
def extract_from_gcs(color, year, month)->Path:
    """Download trip data from GCS"""
    gcs_path = f"/data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("prefect-block-gcp")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"GCP")
    return Path (f"/home/coach/repos/prefect/gcp/{gcs_path}")

@task()
def transform(path:Path)-> pd.DataFrame:
    """return pandas dataframe"""
    df =pd.read_parquet(path)
    print(f"pre:missing passenger count: {df['passenger_count'].isna().sum()}")
    df['passenger_count'].fillna(0, inplace=True)
    print(f"post:missing passenger count: {df['passenger_count'].isna().sum()}")
    return df
@task()
def write_bq(df: pd.DataFrame)-> None:
    """write df to biqgquerey"""

    gcp_credentials_block = GcpCredentials.load("prefect-gcp-creds")

    df.to_gbq(
        destination_table="GCP_prefect.rides",
        project_id="silicon-airlock-376018",
        credentials= gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )

@flow()
def etl_gcs_to_bq():
    """main etl flow to load data into big query"""
    color="yellow"
    year=2021
    month=1

    path = extract_from_gcs(color,year,month)
    df =transform(path)
    write_bq(df)


if __name__ == "__main__":
    etl_gcs_to_bq()
