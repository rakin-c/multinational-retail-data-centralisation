from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

def extract_user_data():
    rds_connector = DatabaseConnector('db_creds.yaml')
    extractor = DataExtractor()
    table = extractor.read_rds_table(rds_connector, 'legacy_users')
    return table

def user_data_cleaning():
    cleaner = DataCleaning()
    user_data = extract_user_data()
    cleaned_user_data = cleaner.clean_user_data(user_data)
    return cleaned_user_data

def write_user_data():
    local_db_connector = DatabaseConnector('sales_data_creds.yaml')
    cleaned_data = user_data_cleaning()
    local_db_connector.upload_to_db(cleaned_data, 'dim_users')

def extract_from_pdf():
    extractor = DataExtractor()
    pdf_table = extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
    return pdf_table

def clean_pdf_data():
    cleaner = DataCleaning()
    pdf_table = extract_from_pdf()
    cleaned_pdf_table = cleaner.clean_card_data(pdf_table)
    return cleaned_pdf_table

def write_card_data():
    local_db_connector = DatabaseConnector('sales_data_creds.yaml')
    cleaned_card_data = clean_pdf_data()
    local_db_connector.upload_to_db(cleaned_card_data, 'dim_card_details')
    
def extract_number_of_stores():
    extractor = DataExtractor()
    number_of_stores = extractor.list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', extractor.api_headers)
    print(f"Number of stores: {number_of_stores}")
    return number_of_stores

def extract_store_data():
    extractor = DataExtractor()
    store_data = extractor.retrieve_stores_data('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details', extractor.api_headers)
    return store_data

def clean_store_data():
    cleaner = DataCleaning()
    store_data = extract_store_data()
    cleaned_store_data = cleaner.clean_store_data(store_data)
    return cleaned_store_data

def write_store_data():
    local_db_connector = DatabaseConnector('sales_data_creds.yaml')
    cleaned_store_data = clean_store_data()
    local_db_connector.upload_to_db(cleaned_store_data, 'dim_store_details')

write_store_data()