import pandas as pd
from database_utils import DatabaseConnector
import tabula
import requests
import json


class DataExtractor:
    '''
    Insert docstring
    '''
    def __init__(self):
        self.api_headers = {
                            'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
                            }

    def read_rds_table(self, connector_instance: DatabaseConnector, table_name: str) -> pd.DataFrame:
        '''
        Insert docstring
        '''
        engine = connector_instance.init_db_engine()
        with engine.connect() as connection:
            table = pd.read_sql_table(table_name, connection)
        return table

    def retrieve_pdf_data(self, link: str) -> pd.DataFrame:
        '''
        Insert docstring
        '''
        pdf_table = tabula.read_pdf(link, pages='all')
        return pd.concat(pdf_table)
    
    def list_number_of_stores(self, number_of_stores_endpoint: str, api_headers: dict) -> int:
        response = requests.get(number_of_stores_endpoint, headers=api_headers)
        if response.status_code == 200:
            return response.json()['number_stores']
        else:
            response_text = json.loads(response.text)
            raise PermissionError(response_text['message'])

    def retrieve_stores_data(self, retrieve_store_endpoint: str, api_headers: dict) -> pd.DataFrame:        
        stores_data = []
        number_of_stores = self.list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', self.api_headers)

        for store_number in range(number_of_stores):
            url = retrieve_store_endpoint + f'{store_number}'
            response = requests.get(url, self.api_headers)
            stores_data.append(response.json())
        
        stores_df = pd.DataFrame.from_records(stores_data)
        return stores_df


if __name__ == '__main__':
    data = DataExtractor()
    connector = DatabaseConnector('db_creds.yaml')
    #pd.set_option('display.max_columns', None)
    print(connector.list_db_tables())
    num_stores = data.list_number_of_stores(f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', data.api_headers)
    
    print(data.retrieve_stores_data(f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/0', data.api_headers))