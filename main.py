from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

def write_user_data():
    rds_connector = DatabaseConnector('db_creds.yaml')
    local_db_connector = DatabaseConnector('sales_data_creds.yaml')
    extractor = DataExtractor()
    data = DataCleaning()

    print(rds_connector.list_db_tables())
    table = extractor.read_rds_table(rds_connector, 'legacy_users')
    cleaned_data = data.clean_user_data(table)

    local_db_connector.upload_to_db(cleaned_data, 'dim_users')

def extract_pdf():
    extractor = DataExtractor()
    pdf_table = extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
    return pdf_table

def clean_pdf_data():
    cleaner = DataCleaning()
    pdf_table = extract_pdf()
    cleaned_pdf_table = cleaner.clean_card_data(pdf_table)
    return cleaned_pdf_table

print(clean_pdf_data())