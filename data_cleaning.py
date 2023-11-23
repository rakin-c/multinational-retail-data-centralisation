import numpy as np
import pandas as pd
from database_utils import DatabaseConnector
from data_extraction import DataExtractor


class DataCleaning:
    '''
    Insert docstring
    '''
    def __init__(self):
        pass

    def clean_user_data(self, user_data: pd.DataFrame) -> pd.DataFrame:
        '''
        Cleans data about the company's users - verifies information is valid and removes NULL values.

        Parameters:
        ----------
        user_data: DataFrame
            The user information extracted from the company's RDS database.
        
        Returns:
        -------
        DataFrame
        '''
        phone_number_mapping = {
                                r'\+44': '0',
                                r'\+49': '0',
                                r'\+1': '',
                                r'\(0\)': '',
                                r'\(': '',
                                r'\)': '',
                                ' ': '',
                                '-': '',
                                r'\.': ''
                                }
        
        uk_number_regex = r'^((\(?0\d{4}\)?\s?\d{3}\s?\d{3})|(\(?0\d{3}\)?\s?\d{3}\s?\d{4})|(\(?0\d{2}\)?\s?\d{4}\s?\d{4}))(\s?\#(\d{4}|\d{3}))?$'
        us_number_regex = r'^((00|\+)1)?(\(?[2-9]\d{2}\)?\d{3}\d{4})(x\d{3,5}$)?'
        de_number_regex = r'^((00|\+)49)?(0?[2-9][0-9]{1,})$'
        
        user_data.replace({'NULL': np.nan, 'GGB': 'GB'}, inplace=True)
        user_data['join_date'] = pd.to_datetime(user_data['join_date'], format='mixed', errors='coerce')
        user_data['date_of_birth'] = pd.to_datetime(user_data['date_of_birth'], format='mixed', errors='coerce')
        user_data.dropna(inplace=True)
        
        user_data['phone_number'].replace(phone_number_mapping, inplace=True, regex=True)
        user_data.loc[((user_data['country_code']=='GB') & (~user_data['phone_number'].str.match(uk_number_regex))) |
                      ((user_data['country_code']=='US') & (~user_data['phone_number'].str.match(us_number_regex))) |
                      ((user_data['country_code']=='DE') & (~user_data['phone_number'].str.match(de_number_regex))), 'phone_number'] = np.nan
        user_data.dropna(inplace=True)

        user_data.drop(['index'], axis=1, inplace=True)
        idx = np.arange(0, len(user_data), 1)
        user_data.set_index(idx, inplace=True)
        user_data.sort_index(ascending=True, inplace=True)
        
        return user_data

    def clean_card_data(self, card_data: pd.DataFrame) -> pd.DataFrame:
        '''
        Cleans company user card data, removing erroneous and NULL values, and verifies correct formatting.

        Parameters:
        ----------
        card_data: DataFrame
            Card information extracted from a PDF document.

        Returns:
        -------
        DataFrame
        '''
        card_data.replace({'NULL': np.nan, }, inplace=True)
        card_data['date_payment_confirmed'] = pd.to_datetime(card_data['date_payment_confirmed'], errors='coerce')
        card_data.loc[~(card_data['expiry_date'].str.match(r'^((0[1-9])|(1[0-2]))\/(\d{2})$', na=True)), 'expiry_date'] = np.nan
        card_data.loc[~(card_data['card_number'].str.match(r'^\d{11,}$', na=True)), 'card_number'] = np.nan
        card_data.dropna(inplace=True)

        idx = np.arange(0, len(card_data), 1)
        card_data.set_index(idx, inplace=True)
        card_data.sort_index(ascending=True, inplace=True)
        
        return card_data
    
    def clean_store_data(self, store_data: pd.DataFrame) -> pd.DataFrame:
        '''
        Insert docstring
        '''
        mapping_dict = {
                        'eeEurope': 'Europe',
                        'eeAmerica': 'America',
                        'NULL': np.nan,
                        None: np.nan,
                        'N/A': np.nan
                        }

        store_data.replace(mapping_dict, inplace=True)
        store_data['staff_numbers'] = store_data['staff_numbers'].str.extract(r'(\d+)')
        store_data['opening_date'] = pd.to_datetime(store_data['opening_date'], format='mixed', errors='coerce')
        store_data['longitude'] = pd.to_numeric(store_data['longitude'], errors='coerce')
        store_data['latitude'] = pd.to_numeric(store_data['latitude'], errors='coerce')
        store_data['staff_numbers'] = pd.to_numeric(store_data['staff_numbers'])
        
        store_data.dropna(subset=['opening_date'], inplace=True)
        store_data.drop(['lat'], axis=1, inplace=True)

        store_data.drop(['index'], axis=1, inplace=True)
        idx = np.arange(0, len(store_data), 1)
        store_data.set_index(idx, inplace=True)
        store_data.sort_index(ascending=True, inplace=True)

        return store_data


if __name__ == '__main__':
    rds_connector = DatabaseConnector('db_creds.yaml')
    local_db_connector = DatabaseConnector('sales_data_creds.yaml')
    extractor = DataExtractor()
    cleaner = DataCleaning()
    
    #print(rds_connector.list_db_tables())
    #table = extractor.read_rds_table(rds_connector, 'legacy_users')
    #cleaned_user_data = cleaner.clean_user_data(table)
    #print(cleaned_user_data)
    
    #pdf_table = extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
    #cleaned_card_data = cleaner.clean_card_data(pdf_table)
    #print(cleaned_card_data)

    stores_data = extractor.retrieve_stores_data('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details', extractor.api_headers)
    print(cleaner.clean_store_data(stores_data))