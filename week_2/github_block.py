from prefect.filesystems import GitHub
from prefect import flow,task

@task(log_prints=True)
def download_flow_code(folder: str) -> None:
    """Download flow code folder from Github"""
    github_block = GitHub.load("github")

    github_block.get_directory(folder)

    github_block.save('flow')

@flow(log_prints=True)
def main_flow(months: list[int] = [2,3],color: str = 'yellow', year: int = 2019):
    """Main flow"""
    download_flow_code("etl_web_to_gcs_hw.py")

    from flow.etl_web_to_gcs_hw import etl_gcs_to_bq

    etl_gcs_to_bq(months=months,color=color,year=year)

if __name__ == "__main__":
    main_flow(months=[11],color='green',year=2020)
