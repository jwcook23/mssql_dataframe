from datetime import datetime

import pytest
import pandas as pd

from mssql_dataframe import helpers, connect, create, modify


@pytest.fixture(scope="module")
def connection():

    db = connect.SQLServer(database_name='tempdb', server_name='localhost', autocommit=False)
    yield db
    db.connection.close()


def test_column_input_error(connection):

    table_name = '##column_input_error'
    columns = {"A": "VARCHAR", "B": "VARCHAR"}
    create.table(connection, table_name, columns)
    
    with pytest.raises(ValueError):
        modify.column(connection,table_name, modify='delete', column_name='B')


def test_column_drop(connection):

    table_name = '##column_drop'
    columns = {"A": "VARCHAR", "B": "VARCHAR"}
    create.table(connection, table_name, columns)
    
    modify.column(connection,table_name, modify='drop', column_name='B')
    schema = helpers.get_schema(connection, table_name)
    assert 'B' not in schema.index


def test_column_add(connection):

    table_name = '##test_column_add'
    columns = {"A": "VARCHAR"}
    create.table(connection, table_name, columns)

    modify.column(connection, table_name, modify='add', column_name='B', data_type='VARCHAR(20)')
    schema = helpers.get_schema(connection, table_name)
    assert 'B' in schema.index
    assert schema.at['B','data_type']=='varchar'
    assert schema.at['B','max_length']==20

    modify.column(connection, table_name, modify='add', column_name='C', data_type='BIGINT')
    schema = helpers.get_schema(connection, table_name)
    assert 'C' in schema.index
    assert schema.at['C','data_type']=='bigint' 


def test_column_alter(connection):

    table_name = '##test_column_alter'
    columns = {"A": "VARCHAR(10)", "B": "BIGINT", "C": "BIGINT", "D": "BIGINT"}
    create.table(connection, table_name, columns)

    modify.column(connection, table_name, modify='alter', column_name='B', data_type='INT')
    schema = helpers.get_schema(connection, table_name)
    assert 'B' in schema.index
    assert schema.at['B','data_type']=='int'
    assert schema.at['B', 'is_nullable']==True

    modify.column(connection, table_name, modify='alter', column_name='C', data_type='INT', not_null=True)
    schema = helpers.get_schema(connection, table_name)
    assert 'C' in schema.index
    assert schema.at['C','data_type']=='int'
    assert schema.at['C', 'is_nullable']==False


def test_primary_key_input_error(connection):

    table_name = '##test_primary_key_input_error'
    columns = {"A": "INT", "B": "BIGINT", "C": "BIGINT", "D": "BIGINT"}
    create.table(connection, table_name, columns, not_null=["A","B"])

    with pytest.raises(ValueError):
        modify.primary_key(connection, table_name, modify='create', columns=['A','B'], primary_key_name = '_pk_1') 


def test_primary_key_one_column(connection):

    table_name = '##test_primary_key_one_column'
    columns = {"A": "INT", "B": "BIGINT", "C": "BIGINT", "D": "BIGINT"}
    create.table(connection, table_name, columns, not_null=["A","B"])

    modify.primary_key(connection, table_name, modify='add', columns='A', primary_key_name = '_pk_1')
    schema = helpers.get_schema(connection, table_name)
    assert schema.at['A','is_primary_key']==True
    assert sum(schema['is_primary_key'])==1

    modify.primary_key(connection, table_name, modify='drop', columns='A',  primary_key_name = '_pk_1')
    schema = helpers.get_schema(connection, table_name)
    assert schema.at['A','is_primary_key']==False
    assert sum(schema['is_primary_key'])==0


def test_primary_key_two_columns(connection):

    table_name = '##test_primary_key_two_columns'
    columns = {"A": "INT", "B": "BIGINT", "C": "BIGINT", "D": "BIGINT"}
    create.table(connection, table_name, columns, not_null=["A","B"])

    modify.primary_key(connection, table_name, modify='add', columns=['A','B'], primary_key_name = '_pk_1')
    schema = helpers.get_schema(connection, table_name)
    assert schema.at['A','is_primary_key']==True
    assert schema.at['B','is_primary_key']==True
    assert sum(schema['is_primary_key'])==2

    modify.primary_key(connection, table_name, modify='drop', columns=['A','B'],  primary_key_name = '_pk_1')
    schema = helpers.get_schema(connection, table_name)
    assert schema.at['A','is_primary_key']==False
    assert schema.at['B','is_primary_key']==False
    assert sum(schema['is_primary_key'])==0