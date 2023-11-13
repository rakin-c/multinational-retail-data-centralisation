import pandas as pd
from database_utils import DatabaseConnector
import tabula


class DataExtractor:
    '''
    Insert docstring
    '''
    def __init__(self):
        pass

    def read_rds_table(self, connector_instance: DatabaseConnector, table_name: str) -> pd.DataFrame:
        '''
        Insert docstring
        '''
        engine = connector_instance.init_db_engine()
        with engine.connect() as connection:
            table = pd.read_sql_table(table_name, connection)
        return table

    def retrieve_pdf_data(self, link: str) -> pd.DataFrame:
        '''
        Insert docstring
        '''
        pdf_table = tabula.read_pdf(link, pages='all')
        return pd.concat(pdf_table)

if __name__ == '__main__':
    data = DataExtractor()
    connector = DatabaseConnector('db_creds.yaml')
    #pd.set_option('display.max_columns', None)
    print(connector.list_db_tables())
    print(data.read_rds_table(connector, 'legacy_users'))
