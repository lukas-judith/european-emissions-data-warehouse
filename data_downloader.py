import requests


class DataDownloader():
    """
    Downloader for EEA GHG emission projections.
    """
    def __init__(self):
        """
        Set a fixed URL and name for the data to be downloaded.        
        """
        # endpoint for .csv file
        # URL last confirmed on 30/05/2023
        self.url = 'https://sdi.eea.europa.eu/datashare/s/GYJfBm2fMr5P6Be/download?path=&files=GHG_projections_2022_EEA_csv.csv'
        self.data_name = 'eu_ghg_projections'

    def download_emission_data(self):
        """
        Downloads dataset on greenhouse emissions from EEA website.
        """
        response = requests.get(self.url)
        
        if response.status_code == 200:
            file_name = f'{self.data_name}.csv'
            print(f'Successfully downloaded {self.data_name} dataset from EEA website!')
            with open(file_name, 'w', encoding='utf-8') as file:
                file.write(response.text)
        else:
            file_name = None
            print(f'Error! Request failed with status code {response.status_code}!')

        return file_name


if __name__=='__main__':
    downloader = DataDownloader()
    downloader.download_emission_data()