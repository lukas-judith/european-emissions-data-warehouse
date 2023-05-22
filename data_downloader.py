import pdb
import requests
import pandas as pd

from urllib.parse import urljoin


# TODO: include flag/argument to decide whether to keep downloaded dataset or retrieve newer version

def download_emission_data():
    """
    Download dataset on greenhouse emissions from EEA website.
    """
    # endpoint for .csv file
    url = 'https://sdi.eea.europa.eu/datashare/s/GYJfBm2fMr5P6Be/download?path=&files=GHG_projections_2022_EEA_csv.csv'
    name = 'eu_ghg_projections'

    response = requests.get(url)

    if response.status_code == 200:
        file_name = f'{name}.csv'
        print(f'Successfully downloaded {name} dataset from EEA website!')
        with open(file_name, 'w', encoding='utf-8') as file:
        #with open('test.csv', 'w', encoding='utf-8') as file:
            file.write(response.text)
    else:
        file_name = None
        print(f'Error! Request failed with status code {response.status_code}!')

    return file_name


if __name__=='__main__':
    download_emission_data()