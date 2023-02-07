from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(log_prints=True)
def extract_from_gcs(color:str, year:int, month:int)->Path:
    """Download trip data from GCS"""
    # Setup the name / path of file on gcs
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    gcs_path = Path(f"data/{color}/{dataset_file}.parquet")

    # Use prefect block to load credentials
    gcs_block = GcsBucket.load("gcscredential")

    # Download data using path
    gcs_block.get_directory(from_path=gcs_path)

    # Return path of downloaded data on local machine
    return Path(f"./{gcs_path}")

@task(log_prints=True)
def transform(path:Path)-> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)

    return df

@task(log_prints=True,retries=3)
def write_bq(df:pd.DataFrame)->None:
    """Write DataFrame to BigQuery"""
    # Get gcp credentials from prefect block
    gcp_credentials_block = GcpCredentials.load("gcpcred")

    df.to_gbq(
        destination_table="de-zoomcamp-2023-375214.dezdataset.rides",
        project_id="de-zoomcamp-2023-375214",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500000,
        if_exists='append'
    )
    return

@flow(log_prints=True)
def etl_gcs_to_bq(months: list[int] = [2,3],color: str = 'yellow', year: int = 2019):
    """Main ETL flow to load data into Big Query"""

    for month in months:
        path = extract_from_gcs(color,year,month)
        df = transform(f"{path}")
        print(df.shape[0])
        write_bq(df)

if __name__ == "__main__":
    etl_gcs_to_bq(months=[2,3],color='yellow',year=2019)