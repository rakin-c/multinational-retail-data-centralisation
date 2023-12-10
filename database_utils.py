import pandas as pd
import yaml
from sqlalchemy import Engine, create_engine, inspect


class DatabaseConnector:
    '''
    A class overseeing connection to, reading from, and uploading to SQL databases.

    Attributes:
    ----------
    db_creds: str
        The credentials required to log in to the database.
        The login credentials must contain a host name, password, username, database name, and port in the form:
            HOST: host_name
            PASSWORD: password
            USER: username
            DATABASE: database_name
            PORT: port number, usually 5432
        File format must be yaml.
    
    Methods:
    -------
    read_db_creds()
        Reads the database credentials from a yaml file into a python dictionary.
    init_db_engine()
        Creates an engine used to connect to the database.
    list_db_tables()
        Extracts the table names from the database.
    upload_to_db(df, table_name)
        Connects to database and writes a DataFrame to a table.
    '''
    def __init__(self, filepath: str):
        self.db_creds = self._read_db_creds(filepath)

    def _read_db_creds(self):
        '''
        Reads the database credentials from a yaml file into a python object. Should return a dictionary.

        See Also:
        --------
        yaml.safe_load
        '''
        with open(self.filepath, 'r') as file:
            db_creds = yaml.safe_load(file)
        return db_creds
    
    def init_db_engine(self) -> Engine:
        '''
        Creates an engine using the database credentials which is used to connect to the database.

        Returns:
        -------
        Engine

        See Also:
        --------
        sqlalchemy.create_engine: Module and function used to create the engine
        '''
        DATABASE_TYPE = "postgresql"
        DBAPI = "psycopg2"
        USER = self.db_creds['USER']
        PASSWORD = self.db_creds['PASSWORD']
        HOST = self.db_creds['HOST']
        PORT = self.db_creds['PORT']
        DATABASE = self.db_creds['DATABASE']

        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return engine
    
    def list_db_tables(self) -> list:
        '''
        Extracts the table names from the database.

        Returns:
        -------
        list
        '''
        engine = self.init_db_engine()
        with engine.connect() as connection:
            inspector = inspect(engine)
        return inspector.get_table_names()

    def upload_to_db(self, df: pd.DataFrame, table_name: str):
        '''
        Connects to database and writes a DataFrame to a table. Replaces existing data if table name already exists.

        Parameters:
        ----------
        df: DataFrame
            DataFrame to be written to an table in an SQL database.
        table_name: str
            Name of the table in the database to upload the DataFrame to.
        '''
        try:
            engine = self.init_db_engine()
            with engine.connect() as connection:
                df.to_sql(table_name, connection, if_exists='replace')
                print(f"Uploaded to {table_name}")
        except Exception as exception:
            print(f"An error occurred: {exception}")