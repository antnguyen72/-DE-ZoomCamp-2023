from prefect.filesystems import GitHub
from prefect import flow,task
import pandas as pd
import requests
import os

@task(log_prints=True)
def download_flow_code(file:str) -> None:
    """Download flow code folder from Github"""
    github_block = GitHub.load("github")
    
    print(github_block.repository+file)

    r= requests.get(github_block.repository+file).content
    
    with open('script.py','w') as file:
    	file.write(r.decode('utf-8'))

@flow(log_prints=True)
def main_flow(file:str):
    """Main flow"""
    download_flow_code(file)

    from script import elt_web_to_gcs
    
    etl_web_to_gcs(months=[11],color='green',year=2020)
    
    os.remove('script.py')
    
if __name__ == "__main__":
    main_flow('etl_web_to_gcs_hw.py')
