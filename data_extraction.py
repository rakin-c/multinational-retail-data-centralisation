import pandas as pd
from database_utils import DatabaseConnector


class DataExtractor:
    '''
    Insert docstring
    '''
    def __init__(self):
        pass

    def read_rds_table(self, connector_instance: DatabaseConnector, table_name: str):
        '''
        Insert docstring
        '''
        engine = connector_instance.init_db_engine()
        with engine.connect() as connection:
            table = pd.read_sql_table(table_name, connection)
        return table


if __name__ == '__main__':
    data = DataExtractor()
    connector = DatabaseConnector('db_creds.yaml')
    #pd.set_option('display.max_columns', None)
    print(connector.list_db_tables())
    print(data.read_rds_table(connector, 'legacy_users'))
