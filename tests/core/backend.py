import pytest
import pandas as pd

from mssql_dataframe.core.backend import SQLServer


@pytest.fixture(scope="module")
def dataframes():
    dataframes = {}
    dataframes['_simple_table'] = pd.DataFrame([
        ['name1',1,1,1,1,1,'11/01/2001','01:00:00','11/01/2001 01:00:00'],
        ['name2',2,2,2,2,2,'2001-11-01','00:01:00','2001-11-01 00:01:00'],
        ['name3',3,3,3,3,3,'11-01-2001','00:00:01','11-01-2001 00:00:01'],
        ['name4',4,400,2147483648,'4.1','4.11111111111111111111','11/01/2001','01:00:00','11/01/2001 01:00:00']
        ],columns=['_default','_tinyint','_int','_bigint','_numeric','_float','_date','_time','_datetime'])
    return dataframes


@pytest.fixture(scope="module")
def connection():
    db = SQLServer(database_name='master', server_name='localhost')
    yield db
    db.engine.close()

def test_create_table(connection, dataframes):
    connection.create_table('_simple_table', dataframes['_simple_table'])

# import pandas as pd
# import sqlalchemy as sql


# def test_SQLServer():
#     db = SQLServer('AdventureWorks2019')

# test_SQLServer()

# dataframe = pd.DataFrame([
#     [1, 'name1', 'add1', 11],
#     [2, 'name2', 'add2', 12],
#     [3, 'name3', 'add3', 13],
#     [4, 'name4', 'add4', 14],
# ],columns = ['RowID','Name','Address','Value'])


# # Main Table
# table = sql.Table('_test', db.metadata, 
#     sql.Column('RowID', sql.NVARCHAR(None)),
#     sql.Column('Name', sql.NVARCHAR(None)),
#     sql.Column('Address', sql.NVARCHAR(None)),
#     sql.Column('Value', sql.NVARCHAR(None)),
#     schema='dbo')
# table.drop(db.engine, checkfirst=True)
# table.create(db.engine)

# connection = db.engine.connect()
# connection.execute(table.insert(), [
#     {'RowID':1, 'Name':'name1', 'Address':'add1', 'Value':11},
#     {'RowID':2, 'Name':'name2', 'Address':'add2', 'Value':12},
#     {'RowID':3, 'Name':'name3', 'Address':'add3', 'Value':13},
#     {'RowID':4, 'Name':'name4', 'Address':'add4', 'Value':14},
# ])

# table = db.Table('dbo.DatabaseLog',metadata)

# table.columns.constraints