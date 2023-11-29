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

    def clean_user_data(self, users_df: pd.DataFrame) -> pd.DataFrame:
        '''
        Cleans data about the company's users - verifies information is valid and removes NULL values.

        Parameters:
        ----------
        users_df: DataFrame
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

        mapping_dict = {
            'NULL': np.nan,
            'GGB': 'GB'
        }
        
        uk_number_regex = r'^((\(?0\d{4}\)?\s?\d{3}\s?\d{3})|(\(?0\d{3}\)?\s?\d{3}\s?\d{4})|(\(?0\d{2}\)?\s?\d{4}\s?\d{4}))(\s?\#(\d{4}|\d{3}))?$'
        us_number_regex = r'^((00|\+)1)?(\(?[2-9]\d{2}\)?\d{3}\d{4})(x\d{3,5}$)?'
        de_number_regex = r'^((00|\+)49)?(0?[2-9][0-9]{1,})$'
        
        users_df.replace(mapping_dict, inplace=True)
        users_df['join_date'] = pd.to_datetime(users_df['join_date'], format='mixed', errors='coerce')
        users_df['date_of_birth'] = pd.to_datetime(users_df['date_of_birth'], format='mixed', errors='coerce')
        users_df.dropna(inplace=True)
        
        users_df['phone_number'].replace(phone_number_mapping, inplace=True, regex=True)
        users_df.loc[((users_df['country_code']=='GB') & (~users_df['phone_number'].str.match(uk_number_regex))) |
                      ((users_df['country_code']=='US') & (~users_df['phone_number'].str.match(us_number_regex))) |
                      ((users_df['country_code']=='DE') & (~users_df['phone_number'].str.match(de_number_regex))), 'phone_number'] = np.nan
        users_df.dropna(inplace=True)

        users_df.drop(['index'], axis=1, inplace=True)
        idx = np.arange(0, len(users_df), 1)
        users_df.set_index(idx, inplace=True)
        users_df.sort_index(ascending=True, inplace=True)
        
        return users_df

    def clean_card_data(self, cards_df: pd.DataFrame) -> pd.DataFrame:
        '''
        Cleans company user card data, removing erroneous and NULL values, and verifies correct formatting.

        Parameters:
        ----------
        cards_df: DataFrame
            Card information extracted from a PDF document.

        Returns:
        -------
        DataFrame
        '''
        mapping_dict = {
            'NULL': np.nan
        }

        cards_df.replace(mapping_dict, inplace=True)
        cards_df['date_payment_confirmed'] = pd.to_datetime(cards_df['date_payment_confirmed'], errors='coerce')
        cards_df.loc[~(cards_df['expiry_date'].str.match(r'^((0[1-9])|(1[0-2]))\/(\d{2})$', na=True)), 'expiry_date'] = np.nan
        cards_df.loc[~(cards_df['card_number'].str.match(r'^\d{11,}$', na=True)), 'card_number'] = np.nan
        cards_df.dropna(inplace=True)

        idx = np.arange(0, len(cards_df), 1)
        cards_df.set_index(idx, inplace=True)
        cards_df.sort_index(ascending=True, inplace=True)
        
        return cards_df
    
    def clean_store_data(self, stores_df: pd.DataFrame) -> pd.DataFrame:
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

        stores_df.replace(mapping_dict, inplace=True)
        stores_df['staff_numbers'] = stores_df['staff_numbers'].str.extract(r'(\d+)')
        stores_df['opening_date'] = pd.to_datetime(stores_df['opening_date'], format='mixed', errors='coerce')
        stores_df['longitude'] = pd.to_numeric(stores_df['longitude'], errors='coerce')
        stores_df['latitude'] = pd.to_numeric(stores_df['latitude'], errors='coerce')
        stores_df['staff_numbers'] = pd.to_numeric(stores_df['staff_numbers'])
        
        stores_df.dropna(subset=['opening_date'], inplace=True)
        stores_df.drop(['lat'], axis=1, inplace=True)

        stores_df.drop(['index'], axis=1, inplace=True)
        idx = np.arange(0, len(stores_df), 1)
        stores_df.set_index(idx, inplace=True)
        stores_df.sort_index(ascending=True, inplace=True)

        return stores_df
    
    def convert_product_weights(self, products_df: pd.DataFrame) -> pd.DataFrame:
        '''
        Insert docstring
        '''
        mapping_dict = {
            ' ': ''
        }

        products_df['weight'].replace({' ': ''}, inplace=True, regex=True)
        products_df.dropna(inplace=True)

        products_df.loc[products_df['weight'].str.contains('x') == True, 'weight'] = products_df.loc[products_df['weight'].str.contains('x') == True, 'weight'].str.rstrip('g').str.split('x')
        prod_list = []
        for item in products_df['weight']:
            if type(item) == list:
                item = list(map(float, item))
                prod = np.prod(item)
                prod_list.append(prod)
        prod_list = [i/1000 for i in prod_list]
        products_df.loc[products_df['weight'].apply(type) == list, 'weight'] = prod_list

        products_df.loc[products_df['weight'].str.contains('ml') == True, 'weight'] = products_df.loc[products_df['weight'].str.contains('ml') == True, 'weight'].str.rstrip('ml').astype('float', errors='raise').apply(lambda x: (x/1000))
        products_df.loc[products_df['weight'].str.contains('oz') == True, 'weight'] = products_df.loc[products_df['weight'].str.contains('oz') == True, 'weight'].str.rstrip('oz').astype('float', errors='raise').apply(lambda x: (x/35.274))
        products_df.loc[products_df['weight'].str.match(r'.*\dg$') == True, 'weight'] = products_df.loc[products_df['weight'].str.match(r'.*\dg$') == True, 'weight'].str.rstrip('g').astype('float', errors='raise').apply(lambda x: (x/1000))
        products_df.loc[products_df['weight'].str.contains('kg') == True, 'weight'] = products_df.loc[products_df['weight'].str.contains('kg') == True, 'weight'].str.rstrip('kg').astype('float')

        products_df['weight'] = pd.to_numeric(products_df['weight'], errors='coerce')

        return products_df

    def clean_products_data(self, products_df: pd.DataFrame) -> pd.DataFrame:
        '''
        Insert docstring
        '''
        if products_df['weight'].dtype == 'object':    
            products_df = self.convert_product_weights(products_df)
        products_df.dropna(inplace=True)
        products_df = products_df[products_df['removed'] != 'Removed']

        products_df['date_added'] = pd.to_datetime(products_df['date_added'], errors='raise', format='mixed', yearfirst=True)
        products_df.sort_values(['date_added'], inplace=True, ascending=True)
        products_df.drop_duplicates(subset=['product_name'], inplace=True, keep='last')

        products_df['product_price'] = products_df['product_price'].str.extract(r'(\d+\.\d{2})$')
        products_df['product_price'] = pd.to_numeric(products_df['product_price'], errors='coerce')

        products_df.sort_index(ascending=True, inplace=True)
        products_df.drop(['Unnamed: 0'], axis=1, inplace=True)
        idx = np.arange(0, len(products_df), 1)
        products_df.set_index(idx, inplace=True)
        products_df.sort_index(ascending=True, inplace=True)
        
        return products_df

    def clean_orders_data(self, orders_df: pd.DataFrame) -> pd.DataFrame:
        '''
        Insert docstring
        '''
        orders_df.drop(['level_0', 'index', 'first_name', 'last_name', '1'], axis=1, inplace=True)
        
        return orders_df


if __name__ == '__main__':
    rds_connector = DatabaseConnector('db_creds.yaml')
    local_db_connector = DatabaseConnector('sales_data_creds.yaml')
    extractor = DataExtractor()
    cleaner = DataCleaning()
    #pd.set_option('display.max_columns', None)
    
    print(rds_connector.list_db_tables())
    '''
    table = extractor.read_rds_table(rds_connector, 'legacy_users')
    cleaned_user_data = cleaner.clean_user_data(table)
    print(cleaned_user_data)

    pdf_table = extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
    cleaned_card_data = cleaner.clean_card_data(pdf_table)
    print(cleaned_card_data)

    stores_data = extractor.retrieve_stores_data('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details', extractor.api_headers)
    print(cleaner.clean_store_data(stores_data))

    products_df = extractor.extract_from_s3('s3://data-handling-public/products.csv', 'products.csv')
    print(cleaner.clean_products_data(products_df))
    '''
    orders_table = extractor.read_rds_table(rds_connector, 'orders_table')
    print(cleaner.clean_orders_data(orders_table))