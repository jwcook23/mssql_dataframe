import pytest
import pandas as pd

from mssql_dataframe import connect
from mssql_dataframe.core import errors, create, conversion
from mssql_dataframe.core.write import insert

class package:
    def __init__(self, connection):
        self.connection = connection
        self.create = create.create(connection)
        self.insert = insert.insert(connection)

@pytest.fixture(scope="module")
def sql():
    db = connect.connect(database_name='tempdb', server_name='localhost')
    yield package(db)
    db.connection.close()


def test_insert_errors(sql):

    table_name = '##test_errors'

    sql.create.table(table_name, columns={
            'ColumnA': 'SMALLINT',
            'ColumnB': 'VARCHAR(1)'
    })
    sql.connection.connection.commit()

    with pytest.raises(errors.SQLTableDoesNotExist):
        dataframe = pd.DataFrame({'ColumnA': [1]})
        sql.insert.insert('error'+table_name, dataframe=dataframe, include_timestamps=False)

    with pytest.raises(errors.SQLColumnDoesNotExist):
        dataframe = pd.DataFrame({'ColumnC': [1]})
        sql.insert.insert(table_name, dataframe=dataframe, include_timestamps=False)

    with pytest.raises(errors.SQLInsufficientColumnSize):
        dataframe = pd.DataFrame({'ColumnB': ['aaa']})
        sql.insert.insert(table_name, dataframe=dataframe, include_timestamps=False)

    with pytest.raises(errors.SQLInsufficientColumnSize):
        sql.insert.insert(table_name, dataframe=pd.DataFrame({'ColumnA': [100000]}), include_timestamps=False)

    # values that cannot be converted to their Python equalivant based on SQL data type
    with pytest.raises(errors.DataframeInvalidDataType):
        sql.insert.insert(table_name, dataframe=pd.DataFrame({'ColumnA': ['abs']}), include_timestamps=False)
    with pytest.raises(errors.DataframeInvalidDataType):
        sql.insert.insert(table_name, dataframe=pd.DataFrame({'ColumnA': ['12-5']}), include_timestamps=False)
    with pytest.raises(errors.DataframeInvalidDataType):
        sql.insert.insert(table_name, dataframe=pd.DataFrame({'ColumnA': ['12345-67589']}), include_timestamps=False)


def test_dataframe(sql):

    table_name = '##test_dataframe'

    # sample data
    dataframe = pd.DataFrame({
        '_bit': pd.Series([1, 0, None], dtype='boolean'),
        '_tinyint': pd.Series([0, 255, None], dtype='UInt8'),
        '_smallint': pd.Series([-2**15, 2**15-1, None], dtype='Int16'),
        '_int': pd.Series([-2**31, 2**31-1, None], dtype='Int32'),
        '_bigint': pd.Series([-2**63, 2**63-1, None], dtype='Int64'),
        '_float': pd.Series([-1.79**308, 1.79**308, None], dtype='float'),
        '_time': pd.Series(['00:00:00.0000000', '23:59:59.9999999', None], dtype='timedelta64[ns]'),
        '_date': pd.Series([(pd.Timestamp.min+pd.DateOffset(days=1)).date(), pd.Timestamp.max.date(), None], dtype='datetime64[ns]'),
        '_datetime2': pd.Series([pd.Timestamp.min, pd.Timestamp.max, None], dtype='datetime64[ns]'),
        '_varchar': pd.Series(['a', 'bbb', None], dtype='string'),
        '_nvarchar': pd.Series([u'100\N{DEGREE SIGN}F', u'company name\N{REGISTERED SIGN}', None], dtype='string'),
    })

    # create table
    columns = {
            '_time_insert': 'DATETIME2',
            '_bit': 'BIT',
            '_tinyint': 'TINYINT',
            '_smallint': 'SMALLINT',
            '_int': 'INT',
            '_bigint': 'BIGINT',
            '_float': 'FLOAT',
            '_time': 'TIME',
            '_date': 'DATE',
            '_datetime2': 'DATETIME2',
            '_varchar': 'VARCHAR',
            '_nvarchar': 'NVARCHAR'
    }
    columns['_varchar'] = columns['_varchar']+'('+str(dataframe['_varchar'].str.len().max())+')'
    columns['_nvarchar'] = columns['_nvarchar']+'('+str(dataframe['_nvarchar'].str.len().max())+')'
    sql.create.table(table_name, columns)

    # insert data
    dataframe, schema = sql.insert.insert(table_name, dataframe)

    # test result
    statement = f'SELECT * FROM {table_name}'
    result = conversion.read_values(statement, schema, sql.connection.connection)
    assert all(result['_time_insert'].notna())
    assert dataframe.equals(result[result.columns.drop('_time_insert')])


def test_singles(sql):

    table_name = '##test_singles'

    # create table
    columns = {
            'ColumnA': 'TINYINT',
            'ColumnB': 'INT',
            'ColumnC': 'DATE',
    }
    sql.create.table(table_name, columns)

    # single value
    dataframe = pd.DataFrame({'ColumnA': [1]})
    dataframe, schema = sql.insert.insert(table_name, dataframe, include_timestamps=False)
    result = conversion.read_values(f'SELECT ColumnA FROM {table_name}', schema, sql.connection.connection)
    assert all(result['ColumnA']==[1])

    # single column
    dataframe = pd.DataFrame({'ColumnB': [2,3,4]})
    dataframe, schema = sql.insert.insert(table_name, dataframe, include_timestamps=False)
    result = conversion.read_values(f'SELECT ColumnB FROM {table_name}', schema, sql.connection.connection)
    assert result['ColumnB'].equals(pd.Series([pd.NA, 2, 3, 4], dtype='Int32'))

    # single column of dates
    dataframe = pd.DataFrame({'ColumnC': ['06-22-2021','06-22-2021']}, dtype='datetime64[ns]')
    dataframe, schema = sql.insert.insert(table_name, dataframe, include_timestamps=False)
    result = conversion.read_values(f'SELECT ColumnC FROM {table_name}', schema, sql.connection.connection)
    assert result['ColumnC'].equals(pd.Series([pd.NA, pd.NA, pd.NA, pd.NA, '06-22-2021','06-22-2021'], dtype='datetime64[ns]'))


def test_composite_pk(sql):
    
    table_name = '##test_insert_composite_pk'

    columns = columns={
            'ColumnA': 'TINYINT',
            'ColumnB': 'VARCHAR(5)',
            'ColumnC': 'BIGINT'
    }
    sql.create.table(table_name, columns, primary_key_column=['ColumnA','ColumnB'])

    dataframe = pd.DataFrame({'ColumnA': [1], 'ColumnB': ['12345'], 'ColumnC': [1]})
    dataframe, schema = sql.insert.insert(table_name, dataframe, include_timestamps=False)

    result = conversion.read_values(f'SELECT * FROM {table_name}', schema, sql.connection.connection)
    assert all(result.index==pd.MultiIndex.from_tuples([(1,'12345')]))
    assert all(result['ColumnC']==1)


def test_add_include_timestamps(sql):

    table_name = '##test_add_include_timestamps'

    # sample data
    dataframe = pd.DataFrame({
        '_bit': pd.Series([1, 0, None], dtype='boolean')
    })

    # create table
    sql.create.table(table_name, columns={'_bit': 'BIT'})
    sql.connection.connection.commit()

    # insert data
    dataframe, schema = sql.insert.insert(table_name, dataframe)

    # test result
    statement = f'SELECT * FROM {table_name}'
    result = conversion.read_values(statement, schema, sql.connection.connection)
    assert all(result['_time_insert'].notna())
    assert result['_bit'].equals(dataframe['_bit'])