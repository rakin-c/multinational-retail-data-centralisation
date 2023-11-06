import pandas as pd
from data_extraction import DataExtractor
from database_utils import DatabaseConnector
import numpy as np
import re


class DataCleaning:
    '''
    Insert docstring
    '''
    def _init_(self):
        pass

    def clean_user_data(self, users: pd.DataFrame):
        '''
        Insert docstring
        '''
        column_names = users.columns
        
        mapping_dict = {'NULL': np.nan, 'GGB': 'GB'}
        phone_number_mapping = {
                                #r'\+44': '0',
                                #r'\+49': '0',
                                #r'\+1': '0',
                                r'\(0\)': '',
                                r'\(': '',
                                r'\)': '',
                                r' ': '',
                                r'-': ''
                                }
        
        #users.set_index("index", inplace=True)
        #users.sort_values("index", ascending=True, inplace=True)
        users['join_date'] = pd.to_datetime(users['join_date'], format='mixed', errors='coerce')
        users['date_of_birth'] = pd.to_datetime(users['date_of_birth'], format='mixed', errors='coerce')
        users.replace(mapping_dict, inplace=True)
        users['phone_number'].replace(phone_number_mapping, inplace=True, regex=True)
        users.dropna(inplace=True)
        print(users)
        '''
        for column in column_names:
            print(users[column].value_counts(), '\n')
        print(users.info(), '\n')
        '''
        

if __name__ == '__main__':
    connector = DatabaseConnector()
    extractor = DataExtractor()
    data = DataCleaning()
    
    legacy_users = extractor.read_rds_table(connector, 'legacy_users')
    #pd.set_option('display.max_columns', None)
    data.clean_user_data(legacy_users, 'users')
    