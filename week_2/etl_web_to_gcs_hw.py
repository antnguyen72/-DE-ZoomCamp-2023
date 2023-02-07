from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    print(df.head(5))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")

    return df

@task(log_prints=True,retries=3)
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write dataframe locally as parquet file"""
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")

    return path

@task(log_prints=True,retries=3)
def write_gcs(path: Path):
    """Upload local parquet file to GCS(Google Cloud Storage)"""
    gcp_cloud_storage_bucket_block = GcsBucket.load("gcscredential")
    gcp_cloud_storage_bucket_block.upload_from_path(
        from_path=f"{path}",
        to_path=path
    )
    return

@flow()
def etl_web_to_gcs(color: str, year: int, months = list[int]) -> None:
    """The main ETL function"""
    for month in months:
        dataset_file = f"{color}_tripdata_{year}-{month:02}"
        dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
        df = fetch(dataset_url)
        counter += df.shape[0]
        path = write_local(df, color, dataset_file)
        write_gcs(path)
    print("THE OPERATION IS DONE FINAL ROW COUNT IS",f"{counter}")

if __name__ == "__main__":
    etl_web_to_gcs()
