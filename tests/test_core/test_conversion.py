import pandas as pd
import pytest

from mssql_dataframe import connect
from mssql_dataframe.core import conversion


table_name = '##_conversion'

@pytest.fixture(scope="module")
def sample():
    ''' Sample data for supported data types.

    key:  SQL data type
    ---
    SQL data type with underscore prefixed

    value: pd.Series([LowerLimit, UpperLimit, NULL, Truncation])
    -----
    LowerLimit: SQL lower limit or pandas lower limit if it is more restrictive
    UpperLimit: SQL upper limit or pandas upper limit if it is more restrictive
    NULL: SQL NULL / pandas <NA>
    Truncation: truncated values due to SQL precision limit

    '''
    df = pd.DataFrame({
        '_bit': pd.Series([False, True, None, None], dtype='boolean'),
        '_tinyint': pd.Series([0, 255, None, None], dtype='UInt8'),
        '_smallint': pd.Series([-2**15, 2**15-1, None, None], dtype='Int16'),
        '_int': pd.Series([-2**31, 2**31-1, None, None], dtype='Int32'),
        '_bigint': pd.Series([-2**63, 2**63-1, None, None], dtype='Int64'),
        '_float': pd.Series([-1.79**308, 1.79**308, None, None], dtype='float'),
        '_time': pd.Series(['00:00:00.0000000', '23:59:59.9999999', None, '00:00:01.123456789'], dtype='timedelta64[ns]'),
        '_date': pd.Series([(pd.Timestamp.min+pd.DateOffset(days=1)).date(), pd.Timestamp.max.date(), None, None], dtype='datetime64[ns]'),
        '_datetime2': pd.Series([pd.Timestamp.min, pd.Timestamp.max, None, pd.Timestamp('1970-01-01 00:00:01.123456789')], dtype='datetime64[ns]'),
        '_varchar': pd.Series(['a', 'bbb', None], dtype='string'),
        '_nvarchar': pd.Series([u'100\N{DEGREE SIGN}F', u'company name\N{REGISTERED SIGN}', None], dtype='string'),
    })

    # increase sample size
    df= pd.concat([df]*10000).reset_index(drop=True)

    # add id column to guarantee SQL read return order
    df.index.name = 'id'
    df = df.reset_index()
    df['id'] = df['id'].astype('Int64')

    return df


@pytest.fixture(scope="module")
def sql(sample):
    # create database cursor
    db = connect.connect(database_name='tempdb', server_name='localhost')
    cursor = db.connection.cursor()
    cursor.fast_executemany = True

    # column name = dataframe column name, data type = dataframe column name without prefixed underscore
    create = {x:x[1::].upper() for x in sample.columns if x!='id'}
    # use string lengths to determine string column size
    create = {k:(v+'('+str(sample[k].str.len().max())+')' if v=='VARCHAR' else v) for k,v in create.items()}
    create = {k:(v+'('+str(sample[k].str.len().max())+')' if v=='NVARCHAR' else v) for k,v in create.items()}
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


def test_rules(sample):

    # insure conversion rules are fully defined
    defined = conversion.rules.notna().all()
    try:
        assert all(defined)
    except AssertionError as error:
        missing = defined[~defined].index.tolist()
        missing = f'conversion not fully defined for: {missing}'
        error.args += (missing)

    # insure sample contains all conversion rules
    defined = conversion.rules['sql'].isin([x[1::] for x in sample.columns])
    try:
        assert all(defined)
    except AssertionError as error:
        missing = conversion.rules.loc[missing,'sql'].tolist()
        missing = f'columns missing from sample dataframe, SQL column names: {missing}'
        error.args += (missing)

    # insure sample contains correct pandas types
    check = pd.Series(sample.dtypes, name='sample')
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

    
def test_sample(sql, sample):

    schema = conversion.get_schema(sql, table_name, columns=sample.columns)
    sql = conversion.prepare_cursor(schema, dataframe=sample, cursor=sql)
    args, dataframe = conversion.prepare_values(schema, dataframe=sample)