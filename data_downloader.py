import pdb
import requests
import pandas as pd

from urllib.parse import urljoin


# TODO: include flag/argument to decide whether to keep downloaded dataset or retrieve newer version

def download_emission_data():
    """
    Use Eurostat API to download latest datasets on greenhouse emissions.
    """
    # lookup table with dataset name and respective dataset code
    dataset_map = {
        'net_greenhouse_gas_emissions' : 'sdg_13_10', # Net greenhouse gas emissions (source: EEA)
    }
    
    # loop over all dataset codes and retrieve the dataset
    # using the eurostat API
    for name, code in dataset_map.items():

        url = f'https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/{code}?format=SDMX-CSV'

        response = requests.get(url)

        if response.status_code == 200:
            print(f'Successfully downloaded {name} dataset')
            #with open(f'{name}.csv', 'w', encoding='utf-8') as file:
            with open('test.csv', 'w', encoding='utf-8') as file:
                file.write(response.text)
        else:
            print(f'Error! Request failed with status code {response.status_code}!')


