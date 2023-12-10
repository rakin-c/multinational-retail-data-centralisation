from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning


class UserData():
    '''
    Class associated with manipulating the company's user data.

    Methods:
    -------
    extract_user_data()

    user_data_cleaning()
    
    write_user_data()
    '''
    def extract_user_data(self):
        rds_connector = DatabaseConnector('db_creds.yaml')
        extractor = DataExtractor()
        users_df = extractor.read_rds_table(rds_connector, 'legacy_users')
        return users_df

    def user_data_cleaning(self):
        cleaner = DataCleaning()
        users_df = self.extract_user_data()
        cleaned_user_data = cleaner.clean_user_data(users_df)
        return cleaned_user_data

    def write_user_data(self):
        local_db_connector = DatabaseConnector('sales_data_creds.yaml')
        cleaned_user_data = self.user_data_cleaning()
        local_db_connector.upload_to_db(cleaned_user_data, 'dim_users')

class CardData():
    '''
    Class associated with the company's card data.

    Methods:
    -------
    extract_card_data()

    card_data_cleaning()

    write_card_data()
    '''
    def extract_card_data(self):
        extractor = DataExtractor()
        cards_df = extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
        return cards_df

    def card_data_cleaning(self):
        cleaner = DataCleaning()
        cards_df = self.extract_card_data()
        cleaned_card_data = cleaner.clean_card_data(cards_df)
        return cleaned_card_data

    def write_card_data(self):
        local_db_connector = DatabaseConnector('sales_data_creds.yaml')
        cleaned_card_data = self.card_data_cleaning()
        local_db_connector.upload_to_db(cleaned_card_data, 'dim_card_details')
    
class StoreData():
    '''
    Class associated with the company's store data.

    Methods:
    -------
    extract_number_of_stores()

    extract_store_data()

    store_data_cleaning()

    write_store_data()
    '''
    def extract_number_of_stores(self):
        extractor = DataExtractor()
        number_of_stores = extractor.list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', extractor.api_headers)
        print(f"Number of stores: {number_of_stores}")
        return number_of_stores

    def extract_store_data(self):
        extractor = DataExtractor()
        stores_df = extractor.retrieve_stores_data('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details', extractor.api_headers)
        return stores_df

    def store_data_cleaning(self):
        cleaner = DataCleaning()
        stores_df = self.extract_store_data()
        cleaned_store_data = cleaner.clean_store_data(stores_df)
        return cleaned_store_data

    def write_store_data(self):
        local_db_connector = DatabaseConnector('sales_data_creds.yaml')
        cleaned_store_data = self.store_data_cleaning()
        local_db_connector.upload_to_db(cleaned_store_data, 'dim_store_details')

class ProductData():
    '''
    Class associated with company's product data.

    Methods:
    -------
    extract_product_data()

    product_data_cleaning()

    write_product_data()
    '''
    def extract_product_data(self):
        extractor = DataExtractor()
        products_df = extractor.extract_from_s3('s3://data-handling-public/products.csv', 'products.csv')
        return products_df

    def product_data_cleaning(self):
        cleaner = DataCleaning()
        products_df = self.extract_product_data()
        products_in_kg = cleaner.convert_product_weights(products_df)
        cleaned_product_data = cleaner.clean_products_data(products_in_kg)
        return cleaned_product_data

    def write_product_data(self):
        local_db_connector = DatabaseConnector('sales_data_creds.yaml')
        cleaned_product_data = self.product_data_cleaning()
        local_db_connector.upload_to_db(cleaned_product_data, 'dim_products')

class OrderData():
    '''
    Class associated with company's order data.

    Methods:
    -------
    extract_order_data()

    order_data_cleaning()

    write_order_data()
    '''
    def extract_order_data(self):
        rds_connector = DatabaseConnector('db_creds.yaml')
        extractor = DataExtractor()
        orders_df = extractor.read_rds_table(rds_connector, 'orders_table')
        return orders_df

    def order_data_cleaning(self):
        cleaner = DataCleaning()
        orders_df = self.extract_order_data()
        cleaned_order_data = cleaner.clean_orders_data(orders_df)
        return cleaned_order_data
    
    def write_order_data(self):
        local_db_connector = DatabaseConnector('sales_data_creds.yaml')
        cleaned_order_data = self.order_data_cleaning()
        local_db_connector.upload_to_db(cleaned_order_data, 'orders_table')

class DatetimeData():
    '''
    Class associated with company's order dates and times data.

    Methods:
    -------
    extract_datetime_data()

    datetime_data_cleaning()

    write_datetime_data()
    '''
    def extract_datetime_data(self):
        extractor = DataExtractor()
        datetimes_df = extractor.extract_from_s3('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json', 'date_details.json')
        return datetimes_df

    def datetime_data_cleaning(self):
        cleaner = DataCleaning()
        datetimes_df = self.extract_datetime_data()
        cleaned_datetime_data = cleaner.clean_datetimes_data(datetimes_df)
        return cleaned_datetime_data
    
    def write_datetime_data(self):
        local_db_connector = DatabaseConnector('sales_data_creds.yaml')
        cleaned_datetime_data = self.datetime_data_cleaning()
        local_db_connector.upload_to_db(cleaned_datetime_data, 'dim_date_times')


def extract_table_names():
    '''
    See:
        DatabaseConnector.list_db_tables()
    '''
    connector = DatabaseConnector('db_creds.yaml')
    print('RDS database tables:', connector.list_db_tables())

if __name__ == '__main__':
    user_data = UserData()
    card_data = CardData()
    store_data = StoreData()
    product_data = ProductData()
    order_data = OrderData()
    datetime_data = DatetimeData()