import pandas as pd
from database_utils import DatabaseConnector


class DataExtractor:
    '''
    Insert docstring
    '''
    def _init_(self):
        pass

    def read_rds_table(self, connector_instance: DatabaseConnector, table_name: str):
        '''
        Insert docstring
        '''
        database_connector = connector_instance
        engine = database_connector.init_db_engine()
        with engine.connect() as connection:
            table = pd.read_sql_table(table_name, connection)
        return table


if __name__ == '__main__':
    data = DataExtractor()
    connector = DatabaseConnector()
    #pd.set_option('display.max_columns', None)
    print(connector.list_db_tables())
    print(data.read_rds_table(connector, 'legacy_users'))