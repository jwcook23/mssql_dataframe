import pyodbc
import sqlalchemy as db


class sql_server():
    '''
    Connect to SQL Server.

    Inputs:
        database_name       str                     name of database
        server_name         str, optional           server to connect with
        driver              str, optional           if not given, find first driver "for SQL Server"
        autocommit          bool, optional          
    Ouputs:
        self.engine         sqlalchemy engine
    '''


    def __init__(self, database_name: str, server_name: str = 'localhost', driver: str = None, autocommit=True):

        if driver is None:
            drivers = [x for x in pyodbc.drivers() if x.endswith(' for SQL Server')]  

        self.engine = db.create_engine('mssql+pyodbc://@'+server_name+'/'+database_name+'?&driver='+drivers[0], fast_executemany=True)