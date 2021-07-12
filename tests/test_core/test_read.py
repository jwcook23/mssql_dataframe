import pytest
import pandas as pd

from mssql_dataframe import connect
from mssql_dataframe.core import create, write, read


class package:
    def __init__(self, connection):
        self.create = create.create(connection)
        self.write = write.write(connection, adjust_sql_objects=False)
        self.read = read.read(connection)

@pytest.fixture(scope="module")
def sql():
    db = connect.connect(database_name='tempdb', server_name='localhost', autocommit=False)
    yield package(db)
    db.connection.close()


def test_select_input_errors(sql):

    table_name = '##test_select_input_errors'
    sql.create.table(table_name, columns={
            'ColumnA': 'TINYINT'
    })

    with pytest.raises(ValueError):
        sql.read.select(table_name, limit='1')

    with pytest.raises(ValueError):
        sql.read.select(table_name, order_column='A', order_direction=None)

    with pytest.raises(ValueError):
        sql.read.select(table_name, order_column='A', order_direction='a')    


def test_select(sql):

    # create table and insert sample data
    table_name = '##test_select'
    sql.create.table(table_name, columns={
            'ColumnA': 'TINYINT',
            'ColumnB': 'INT',
            'ColumnC': 'BIGINT',
            'ColumnD': 'DATETIME',
            'ColumnE': 'VARCHAR(10)'
    }, primary_key_column="ColumnA")

    input = pd.DataFrame({
        'ColumnA': [5,6,7],
        'ColumnB': [5,6,None],
        'ColumnC': [pd.NA,6,7],
        'ColumnD': ['06-22-2021','06-22-2021',pd.NaT],
        'ColumnE' : ['a','b',None]
    })
    input['ColumnB'] = input['ColumnB'].astype('Int64')
    input['ColumnD'] = pd.to_datetime(input['ColumnD'])
    sql.write.insert(table_name, input)

    # all columns and rows
    dataframe = sql.read.select(table_name)
    assert dataframe.index.name=='ColumnA'
    assert dataframe.shape[1]==input.shape[1]-1
    assert dataframe.shape[0]==input.shape[0]
    assert dataframe.dtypes['ColumnB']=='Int32'
    assert dataframe.dtypes['ColumnC']=='Int64'
    assert dataframe.dtypes['ColumnD']=='datetime64[ns]'
    assert dataframe.dtypes['ColumnE']=='object'

    # # optional columns specified
    dataframe = sql.read.select(table_name, column_names=["ColumnB","ColumnC"])
    assert dataframe.index.name=='ColumnA'
    assert all(dataframe.columns==["ColumnB","ColumnC"])
    assert dataframe.shape[0]==input.shape[0]

    # optional where statement
    dataframe = sql.read.select(table_name, column_names=['ColumnB','ColumnC','ColumnD'], where="ColumnB>4 AND ColumnC IS NOT NULL OR ColumnD IS NULL")
    assert sum((dataframe['ColumnB']>4 & dataframe['ColumnC'].notna()) | dataframe['ColumnD'].isna())==2

    # optional limit
    dataframe = sql.read.select(table_name, limit=1)
    assert dataframe.shape[0]==1

    # optional order
    dataframe = sql.read.select(table_name, column_names=["ColumnB"], order_column='ColumnA', order_direction='DESC')
    assert dataframe.index.name=='ColumnA'
    assert all(dataframe.index==[7,6,5])