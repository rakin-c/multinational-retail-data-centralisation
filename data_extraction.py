import pandas as pd
from sqlalchemy import create_engine, inspect
import database_utils


class DataExtractor:
    '''
    Insert docstring
    '''
    def _init_(self):
        pass

    def read_rds_table(self, yaml_file: str):
        '''
        Insert docstring
        '''
        connector = database_utils.DatabaseConnector()
        print(connector.list_db_tables(yaml_file))
        engine = connector.init_db_engine(yaml_file)
        with engine.connect() as connection:
            legacy_users = pd.read_sql_table("legacy_users", connection)
        return legacy_users


if __name__ == '__main__':
    data = DataExtractor()
    pd.set_option('display.max_columns', None)
    print(data.read_rds_table('db_creds.yaml'))