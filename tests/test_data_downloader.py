import os
import requests

# for importing the self-written Python modules, change working dir
if os.path.basename(os.getcwd()) == 'tests':
    os.chdir('..')
from data_downloader import DataDownloader


def test_data_downloader_url():
    """
    Tests downloading the emission data from the EEA website.
    """
    downloader = DataDownloader()
    # request header of the url (without body) to check the download link
    # this only checks the availability of the url and does not download the attached file
    response = requests.head(downloader.url)
    assert response.status_code == 200
