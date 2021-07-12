from datetime import date
import warnings

import pytest
import pandas as pd
import numpy as np

from mssql_dataframe import connect
from mssql_dataframe.collection import SQLServer
from mssql_dataframe.core import errors, helpers


@pytest.fixture(scope="module")
def sql():
    connection = connect.connect(database_name='tempdb', server_name='localhost', autocommit=False)
    yield SQLServer(connection, adjust_sql_objects=True)
    connection.connection.close()


def test_insert_create_table(sql):

    table_name = '##test_insert_create_table'
    dataframe = pd.DataFrame({
        "ColumnA": [1,2]
    })
    with warnings.catch_warnings(record=True) as warn:
        sql.write.insert(table_name, dataframe=dataframe)
        results = sql.read.select(table_name)
        assert len(warn)==1
        assert isinstance(warn[-1].message, errors.SQLObjectAdjustment)
        assert all(results==dataframe)


def test_insert_add_column(sql):

    table_name = '##test_insert_add_column'
    sql.create.table(table_name, columns={
        'ColumnA': 'TINYINT'
    })

    dataframe = pd.DataFrame({'ColumnA': [1], 'ColumnB': [2], 'ColumnC': [3]})

    with warnings.catch_warnings(record=True) as warn:
        sql.write.insert(table_name, dataframe=dataframe)
        results = sql.read.select(table_name)
        assert len(warn)==2
        assert isinstance(warn[0].message, errors.SQLObjectAdjustment)
        assert 'Creating column ColumnB ' in str(warn[0].message)
        assert isinstance(warn[-1].message, errors.SQLObjectAdjustment)
        assert 'Creating column ColumnC ' in str(warn[-1].message)
        assert all(results==dataframe)


def test_insert_alter_column(sql):

    table_name = '##test_insert_alter_column'
    sql.create.table(table_name, columns={
        'ColumnA': 'TINYINT',
        'ColumnB': 'VARCHAR(1)',
        'ColumnC': 'TINYINT'
    })

    dataframe = pd.DataFrame({'ColumnA': [1], 'ColumnB': ['aaa'], 'ColumnC': [100000]})

    with warnings.catch_warnings(record=True) as warn:
        sql.write.insert(table_name, dataframe=dataframe)
        results = sql.read.select(table_name)
        assert len(warn)==2
        assert isinstance(warn[0].message, errors.SQLObjectAdjustment)
        assert 'Altering column ColumnB ' in str(warn[0].message)
        assert isinstance(warn[-1].message, errors.SQLObjectAdjustment)
        assert 'Altering column ColumnC ' in str(warn[-1].message)
        assert all(results==dataframe)

    schema = helpers.get_schema(sql.connection, table_name)
    columns,_,_,_ = helpers.flatten_schema(schema)
    assert columns=={'ColumnA': 'tinyint', 'ColumnB': 'varchar(3)', 'ColumnC': 'int'}


def test_update_create_table(sql):
    pass


def test_update_add_column(sql):
    pass
    # table_name = '##test_update_add_column'

    # # create table to update
    # dataframe = pd.DataFrame({
    #     'ColumnA': [1,2]
    # })
    # dataframe = sql.create.table_from_dataframe(table_name, dataframe, primary_key='index')
    # sql.write.insert(table_name, dataframe)

    # # update values in table, using the primary key created in SQL
    # dataframe['NewColumn'] = [3,4]
    # sql.write.update(table_name, dataframe[['NewColumn']])

    # # test result
    # result = sql.read.select(table_name)
    # expected = pd.DataFrame({'ColumnA': [1,2], 'NewColumn': [3,4]})
    # assert (expected.values==result[['ColumnA','NewColumn']].values).all()
    # assert (result['_time_update'].notna()).all()


def test_update_alter_column(sql):
    pass


def test_merge_create_table(sql):
    pass


def test_merge_add_column(sql):
    pass


def test_merge_alter_column(sql):
    pass