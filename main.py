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
    
write_card_data()