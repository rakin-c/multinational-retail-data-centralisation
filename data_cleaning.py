import pandas as pd
import data_extraction
import regex

class DataCleaning:
    '''
    Insert docstring
    '''
    def _init_(self):
        pass

    def clean_user_data(self, yaml_file: str):
        extractor = data_extraction.DataExtractor()
        legacy_users = extractor.read_rds_table(yaml_file)
        #legacy_users.set_index("index", inplace=True)
        print(legacy_users.columns, '\n')
        print(legacy_users.info(), '\n')
        print(legacy_users.tail(10))


if __name__ == '__main__':
    data = DataCleaning()
    data.clean_user_data('db_creds.yaml')