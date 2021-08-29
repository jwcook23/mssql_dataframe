import pandas as pd
import pytest

from mssql_dataframe import connect
from mssql_dataframe.core import conversion
from . import sample

table_name = '##_conversion'

@pytest.fixture(scope="module")
def data():
    ''' Sample data for supported data types.

    See Also: sample.py
    '''

    df = sample.dataframe

    # increase sample size
    df = pd.concat([df]*10000).reset_index(drop=True)

    # add id column to guarantee SQL read return order
    df.index.name = 'id'
    df = df.reset_index()
    df['id'] = df['id'].astype('Int64')

    return df


@pytest.fixture(scope="module")
def sql(data):
    # create database cursor
    db = connect.connect(database_name='tempdb', server_name='localhost')

    # add output converters
    db.connection = conversion.prepare_connection(db.connection)

    # database cursor
    cursor = db.connection.cursor()
    cursor.fast_executemany = True

    # column name = dataframe column name, data type = dataframe column name without prefixed underscore
    create = {x:x[1::].upper() for x in data.columns if x!='id'}
    # use string lengths to determine string column size
    create = {k:(v+'('+str(data[k].str.len().max())+')' if v=='VARCHAR' else v) for k,v in create.items()}
    create = {k:(v+'('+str(data[k].str.len().max())+')' if v=='NVARCHAR' else v) for k,v in create.items()}
    # create SQL table, including a primary key for SQL read sorting
    create = ',\n'.join([k+' '+v for k,v in create.items()])
    create = f"""
    CREATE TABLE {table_name} (
    id BIGINT PRIMARY KEY,
    {create}
    )"""
    cursor.execute(create)

    yield cursor
    db.connection.close()


def test_rules(data):

    # insure conversion rules are fully defined
    defined = conversion.rules.notna().all()
    try:
        assert all(defined)
    except AssertionError as error:
        missing = defined[~defined].index.tolist()
        missing = f'conversion not fully defined for: {missing}'
        error.args += (missing)

    # insure sample contains all conversion rules
    defined = conversion.rules['sql'].isin([x[1::] for x in data.columns])
    try:
        assert all(defined)
    except AssertionError as error:
        missing = conversion.rules.loc[missing,'sql'].tolist()
        missing = f'columns missing from sample dataframe, SQL column names: {missing}'
        error.args += (missing)

    # insure sample contains correct pandas types
    check = pd.Series(data.dtypes, name='sample')
    check = check.apply(lambda x: x.name)
    check.index = [x[1::] for x in check.index]
    check = conversion.rules.merge(check, left_on='sql', right_index=True)
    defined = check['pandas']==check['sample']
    try:
        assert all(defined)
    except AssertionError as error:
        missing = check.loc[~defined,'sql'].tolist()
        missing = f'columns with wrong data type in sample dataframe, SQL column names: {missing}'
        error.args += (missing)

    
def test_sample(sql, data):

    # get target table schema to setup insert/read
    schema = conversion.get_schema(sql, table_name, columns=data.columns)

    # insert data
    cursor = conversion.prepare_cursor(schema, dataframe=data, cursor=sql)
    dataframe, values = conversion.prepare_values(schema, dataframe=data)
    columns = dataframe.columns
    conversion.insert_values(table_name, columns, values, cursor)

    # read data, excluding ID columns that is only to insure sorting
    columns = ', '.join([x for x in data.columns if x!='id'])
    statement = f'SELECT {columns} FROM {table_name} ORDER BY id ASC'
    result = conversion.read_values(statement, schema, cursor)

    # compare result to insert
    ## note that dataframe is compared instead of sample as dataframe values may have changed during preparation
    assert result.equals(dataframe.drop(columns='id'))