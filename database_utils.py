import pandas as pd
import yaml
from sqlalchemy import create_engine, inspect

class DatabaseConnector:
    '''
    Insert docstring
    '''
    def _init_(self):
        pass

    def read_db_creds(self, yaml_file: str):
        '''
        Reads the database credentials from a yaml file into a python dictionary.

        Parameters:
        ----------
        yaml_file: str
            The yaml file to be parsed into a dictionary.
        '''
        with open(yaml_file, 'r') as file:
            db_creds = yaml.safe_load(file)
        return db_creds
    
    def init_db_engine(self, yaml_file: str):
        '''
        Insert docstring
        '''
        db_creds = self.read_db_creds(yaml_file)
        engine = create_engine(f"postgresql+psycopg2://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}@{db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}")
        return engine
    
    def list_db_tables(self, yaml_file: str):
        '''
        Insert docstring
        '''
        engine = self.init_db_engine(yaml_file)
        with engine.connect() as connection:
            inspector = inspect(engine)
        return inspector.get_table_names()


if __name__ == '__main__':
    data = DatabaseConnector()
    print(data.read_db_creds('db_creds.yaml'))
    print(data.list_db_tables('db_creds.yaml'))