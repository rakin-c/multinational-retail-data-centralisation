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

    def clean_user_data(self, users: pd.DataFrame):
        '''
        Insert docstring
        '''
        mapping_dict = {'NULL': np.nan, 'GGB': 'GB'}
        phone_number_mapping = {
                                r'\+44': '0',
                                r'\+49': '0',
                                r'\+1': '',
                                r'\(0\)': '',
                                r'\(': '',
                                r'\)': '',
                                r' ': '',
                                r'-': '',
                                r'\.': ''
                                }
        
        uk_number_regex = r'^((\(?0\d{4}\)?\s?\d{3}\s?\d{3})|(\(?0\d{3}\)?\s?\d{3}\s?\d{4})|(\(?0\d{2}\)?\s?\d{4}\s?\d{4}))(\s?\#(\d{4}|\d{3}))?$'
        us_number_regex = r'^((00|\+)1)?(\(?[2-9]\d{2}\)?\d{3}\d{4})(x\d{3,5}$)?'
        de_number_regex = r'^((00|\+)49)?(0?[2-9][0-9]{1,})$'
        
        users['join_date'] = pd.to_datetime(users['join_date'], format='mixed', errors='coerce')
        users['date_of_birth'] = pd.to_datetime(users['date_of_birth'], format='mixed', errors='coerce')
        users.replace(mapping_dict, inplace=True)
        users.dropna(inplace=True)
        
        users['phone_number'].replace(phone_number_mapping, inplace=True, regex=True)
        users.loc[((users['country_code']=='GB') & (~users['phone_number'].str.match(uk_number_regex))) | 
                  ((users['country_code']=='US') & (~users['phone_number'].str.match(us_number_regex))) | 
                  ((users['country_code']=='DE') & (~users['phone_number'].str.match(de_number_regex))), 'phone_number'] = np.nan
        
        users.dropna(inplace=True)
        users.set_index("index", inplace=True)
        users.sort_values("index", ascending=True, inplace=True)
        
        return users


if __name__ == '__main__':
    rds_connector = DatabaseConnector('db_creds.yaml')
    local_db_connector = DatabaseConnector('sales_data_creds.yaml')
    extractor = DataExtractor()
    data = DataCleaning()

    print(rds_connector.list_db_tables())
    table = extractor.read_rds_table(rds_connector, 'legacy_users')
    cleaned_data = data.clean_user_data(table)