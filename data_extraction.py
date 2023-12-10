import pandas as pd
from database_utils import DatabaseConnector
import tabula
import requests
import json
import boto3


class DataExtractor:
    '''
    A class containing methods to extract data from multiple sources and read it into DataFrames.

    Attributes:
    ----------
    api_headers: dict
        Headers containing an API key in order to access the store data.

    Methods:
    -------
    read_rds_table(connector_instance, table_name)
        Reads table data from an RDS database.
    retrieve_pdf_data(link)
        Extracts tables from a PDF into a DataFrame.
    list_number_of_stores(number_of_stores_endpoint, api_headers)
        Returns number of stores accessbile via API.
    retrieve_stores_data(retrieve_stores_enpoint, api_headers)
        Extracts store data for each store into a DataFrame.
    extract_from_s3(s3_address, file_name)
        Downloads file from an S3 bucket and reads it into a DataFrame.
    '''
    def __init__(self):
        self.api_headers = {
            'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
        }

    def read_rds_table(self, connector_instance: DatabaseConnector, table_name: str) -> pd.DataFrame:
        '''
        Connects to an RDS database and reads a table from it into a DataFrame.

        Parameters:
        ----------
        connector_instance: DatabaseConnector
            An instance of the DatabaseConnector class, used to connect to the RDS database.
        table_name: str
            Name of the table to be read and extracted.

        Returns:
        -------
        DataFrame
        '''
        engine = connector_instance.init_db_engine()
        with engine.connect() as connection:
            table = pd.read_sql_table(table_name, connection)
        return table

    def retrieve_pdf_data(self, link: str) -> pd.DataFrame:
        '''
        Extracts tables from a PDF document into a DataFrame.

        Parameters:
        ----------
        link: str
            Link to the PDF document.
        
        Returns:
        -------
        DataFrame
        '''
        pdf_table = tabula.read_pdf(link, pages='all')
        return pd.concat(pdf_table)
    
    def list_number_of_stores(self, number_of_stores_endpoint: str, api_headers: dict) -> int:
        '''
        Lists the number of stores that can be accessed via an API endpoint.

        Parameters:
        ----------
        number_of_stores_endpoint: str
            The API endpoint URL which returns the number of stores that the corporation has.
        api_headers: dict
            The API headers must include an API key.

        Returns:
        -------
        int
            Number of stores.
        '''
        response = requests.get(number_of_stores_endpoint, headers=api_headers)
        if response.status_code == 200:
            return response.json()['number_stores']
        else:
            response_text = json.loads(response.text)
            raise PermissionError(response_text['message'])

    def retrieve_stores_data(self, retrieve_store_endpoint: str, api_headers: dict) -> pd.DataFrame:
        '''
        Retrieves the information for each store and collects them into a DataFrame.

        Parameters:
        ----------
        retrieve_stores_endpoint: str
            The API URL ending in '/prod/store_details' from which each store endpoint can be parsed.
        api_headers: dict
            The API headers must include an API key.

        Returns:
        -------
        DataFrame
        '''
        stores_data = []
        number_of_stores = self.list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', api_headers)

        for store_number in range(number_of_stores):
            retrieve_store_endpoint = retrieve_store_endpoint.rstrip('/') #Strip trailing forward slash if given
            url = retrieve_store_endpoint + f'/{store_number}'
            response = requests.get(url, headers=api_headers)
            stores_data.append(response.json())
        
        stores_df = pd.DataFrame(stores_data)
        return stores_df

    def extract_from_s3(self, s3_address: str, file_name: str) -> pd.DataFrame:
        '''
        Downloads file containing product data and extracts information into a DataFrame.

        Parameters:
        ----------
        s3_address: str
            The S3 path of the object to be downloaded and extracted. Can be a URI or a URL.
        file_name: str
            Name of the file to save the S3 object as.

        Returns:
        -------
        DataFrame
        '''
        s3_address_split = s3_address.split('/')
        s3 = boto3.client('s3')

        if s3_address.startswith('s3'):    
            s3_bucket_name = s3_address_split[2]
            s3_object_name = '/'.join(s3_address_split[3:])
            s3.download_file(s3_bucket_name, s3_object_name, file_name)
            if s3_object_name.endswith('csv'):
                with open(file_name, 'r') as file:
                    df = pd.read_csv(file)
            if s3_object_name.endswith('json'):
                with open(file_name, 'r') as file:
                    df = pd.read_json(file)
        
        if s3_address.startswith('https'):
            s3_bucket_name = s3_address_split[2].split('.')[0]
            s3_object_name = '/'.join(s3_address_split[3:])
            s3.download_file(s3_bucket_name, s3_object_name, file_name)
            if s3_object_name.endswith('csv'):
                with open(file_name, 'r') as file:
                    df = pd.read_csv(file)
            if s3_object_name.endswith('json'):
                with open(file_name, 'r') as file:
                    df = pd.read_json(file)

        return df


if __name__ == '__main__':
    data = DataExtractor()
    headers = data.api_headers
    print(headers)