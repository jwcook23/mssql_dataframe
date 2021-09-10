import warnings

import pytest
import pandas as pd

from mssql_dataframe import connect
from mssql_dataframe.core import errors, create, conversion
from mssql_dataframe.core.write import insert

class package:
    def __init__(self, connection):
        self.connection = connection
        self.create = create.create(connection)
        self.insert = insert.insert(connection, adjust_sql_objects=True)

@pytest.fixture(scope="module")
def sql():
    db = connect.connect(database_name='tempdb', server_name='localhost')
    yield package(db)
    db.connection.close()


def test_create_table(sql):

    table_name = '##test_create_table' 

    dataframe = pd.DataFrame({
        "ColumnA": [1,2,3],
        "ColumnB": ['06/22/2021','06-22-2021','2021-06-22']
    })

    with warnings.catch_warnings(record=True) as warn:
        dataframe, schema = sql.insert.insert(table_name, dataframe=dataframe, include_timestamps=True)
        assert len(warn)==3
        assert all([isinstance(x.message, errors.SQLObjectAdjustment) for x in warn])
        assert 'Creating table '+table_name in str(warn[0].message)
        assert 'Created table '+table_name in str(warn[1].message)
        assert 'Creating column _time_insert in table '+table_name in str(warn[2].message)

        statement = f'SELECT * FROM {table_name}'
        result = conversion.read_values(statement, schema, sql.connection.connection)
        expected = pd.DataFrame({
            'ColumnA': pd.Series([1,2,3], dtype='UInt8'),
            'ColumnB' : pd.Series([pd.Timestamp(year=2021, month=6, day=22)]*3, dtype='datetime64[ns]')
        }).set_index(keys='ColumnA')
        assert result[expected.columns].equals(expected)
        assert all(result['_time_insert'].notna())


def test_insert_add_column(sql):

    table_name = '##test_insert_add_column'
    sql.create.table(table_name, columns={
        'ColumnA': 'TINYINT'
    })

    dataframe = pd.DataFrame({'ColumnA': [1], 'ColumnB': [2], 'ColumnC': ['zzz']})

    with warnings.catch_warnings(record=True) as warn:
        sql.insert.insert(table_name, dataframe=dataframe)
        assert len(warn)==3
        assert all([isinstance(x.message, errors.SQLObjectAdjustment) for x in warn])
        assert 'Creating column _time_insert in table '+table_name in str(warn[0].message)        
        assert 'Creating column ColumnB in table '+table_name in str(warn[1].message)
        assert 'Creating column ColumnC in table '+table_name in str(warn[2].message)
        results = sql_adjustable.read.select(table_name)
        assert all(results[['ColumnA','ColumnB','ColumnC']]==dataframe[['ColumnA','ColumnB','ColumnC']])
        assert all(results['_time_insert'].notna())


# def test_insert_alter_column(sql_adjustable): 

#     table_name = '##test_insert_alter_column'
#     sql_adjustable.create.table(table_name, columns={
#         'ColumnA': 'TINYINT',
#         'ColumnB': 'VARCHAR(1)',
#         'ColumnC': 'TINYINT'
#     })

#     dataframe = pd.DataFrame({'ColumnA': [1], 'ColumnB': ['aaa'], 'ColumnC': [100000]})

#     with warnings.catch_warnings(record=True) as warn:
#         sql_adjustable.write.insert(table_name, dataframe=dataframe)
#         assert len(warn)==3
#         assert all([isinstance(x.message, errors.SQLObjectAdjustment) for x in warn])
#         assert 'Creating column _time_insert in table '+table_name in str(warn[0].message)
#         assert 'Altering column ColumnB in table '+table_name in str(warn[1].message)
#         assert 'Altering column ColumnC in table '+table_name in str(warn[2].message)
#         results = sql_adjustable.read.select(table_name)
#         assert all(results[['ColumnA','ColumnB','ColumnC']]==dataframe[['ColumnA','ColumnB','ColumnC']])
#         assert all(results['_time_insert'].notna())

#     schema = conversion.get_schema(sql_adjustable.connection, table_name, columns=dataframe.columns)
#     # columns,_,_,_ = helpers.flatten_schema(schema)
#     assert columns=={'ColumnA': 'tinyint', 'ColumnB': 'varchar(3)', 'ColumnC': 'int', '_time_insert': 'datetime'}


# def test_insert_alter_primary_key(sql_adjustable):

#     table_name = '##test_insert_alter_primary_key'
#     dataframe = pd.DataFrame({
#         'ColumnA': [0,1,2,3],
#         'ColumnB': [0,1,2,3]
#     })
#     dataframe = dataframe.set_index(keys='ColumnA')
#     with warnings.catch_warnings(record=True) as warn:
#         sql_adjustable.create.table_from_dataframe(table_name, dataframe, primary_key='index')
#         assert len(warn)==1
#         assert isinstance(warn[0].message, errors.SQLObjectAdjustment)
#         assert 'Created table' in str(warn[0].message)

#     sql_adjustable.write.insert(table_name, dataframe, include_timestamps=False)

#     new = pd.DataFrame({
#         'ColumnA': [256,257,258,259],
#         'ColumnB': [4,5,6,7]
#     }).set_index(keys='ColumnA')
#     with warnings.catch_warnings(record=True) as warn:
#         sql_adjustable.write.insert(table_name, new, include_timestamps=False)
#         assert len(warn)==1
#         assert all([isinstance(x.message, errors.SQLObjectAdjustment) for x in warn])
#         assert 'Altering column ColumnA in table '+table_name in str(warn[0].message)        
#         results = sql_adjustable.read.select(table_name)
#         assert all(results==dataframe.append(new))


# def test_insert_add_and_alter_column(sql_adjustable):

    # table_name = '##test_insert_add_and_alter_column'
    # dataframe = pd.DataFrame({
    #     'ColumnA': [0,1,2,3],
    #     'ColumnB': [0,1,2,3]
    # })
    # with warnings.catch_warnings(record=True) as warn:
    #     sql_adjustable.create.table_from_dataframe(table_name, dataframe, primary_key='index', row_count=1)
    #     assert len(warn)==1
    #     assert isinstance(warn[0].message, errors.SQLObjectAdjustment)
    #     assert 'Created table' in str(warn[0].message)

    # dataframe['ColumnB'] = [256,257,258,259]
    # dataframe['ColumnC'] = [0,1,2,3]
    # with warnings.catch_warnings(record=True) as warn:
    #     sql_adjustable.write.insert(table_name, dataframe)
    #     assert len(warn)==3
    #     assert all([isinstance(x.message, errors.SQLObjectAdjustment) for x in warn])
    #     assert 'Creating column _time_insert in table '+table_name in str(warn[0].message)        
    #     assert 'Creating column ColumnC in table '+table_name in str(warn[1].message)
    #     assert 'Altering column ColumnB in table '+table_name in str(warn[2].message)        
    #     results = sql_adjustable.read.select(table_name)
    #     assert all(results[['ColumnA','ColumnB','ColumnC']]==dataframe[['ColumnA','ColumnB','ColumnC']])
    #     assert all(results['_time_insert'].notna())