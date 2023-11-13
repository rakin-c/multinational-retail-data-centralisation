from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

rds_connector = DatabaseConnector('db_creds.yaml')
local_db_connector = DatabaseConnector('sales_data_creds.yaml')
extractor = DataExtractor()
data = DataCleaning()

print(rds_connector.list_db_tables())
table = extractor.read_rds_table(rds_connector, 'legacy_users')
cleaned_data = data.clean_user_data(table)

local_db_connector.upload_to_db(cleaned_data, 'dim_users')