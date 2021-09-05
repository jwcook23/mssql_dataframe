import pandas as pd

from mssql_dataframe.core import infer
from . import sample

def _check_dtypes(dtypes):
    '''Assert expected dtypes are inferred. 
    
    Dataframe column names should be in the form _SQLDataType or _SQLDataType_SomeOtherText.
    '''
    expected = dtypes['sql'].reset_index()
    expected['actual'] = expected['column_name'].str.split('_')
    expected['actual'] = expected['actual'].apply(lambda x: x[1])
    assert (expected['sql']==expected['actual']).all()


def _check_dataframe(dataframe, dtypes):
    '''Assert dataframe columns are of the correct type.'''
    expected = dataframe.dtypes.apply(lambda x: x.name)
    expected.name = 'pandas'
    expected.index.name = 'column_name'
    expected = expected.reset_index()
    expected = expected.sort_values(by='column_name', ignore_index=True)
    actual = dtypes['pandas'].reset_index()
    actual = actual.sort_values(by='column_name', ignore_index=True)
    assert actual.equals(expected)

def test_dtypes():

    # setup test data
    dataframe = sample.dataframe
    na = dataframe.isna()
    dataframe = sample.dataframe.astype('str')
    dataframe[na] = None
    dataframe['_time'] = dataframe['_time'].str.replace('0 days ','')

    # infer SQL properties
    dataframe, dtypes, notnull, pk = infer.sql(dataframe)

    # assert inferred results
    _check_dtypes(dtypes)
    _check_dataframe(dataframe, dtypes)
    assert len(notnull)==0
    assert pk is None


def test_pk():

    # setup test data
    dataframe = sample.dataframe
    dataframe = dataframe[dataframe.notna().all(axis='columns')]
    dataframe['_tinyint_smaller'] = pd.Series(range(0,len(dataframe)), dtype='UInt8')
    dataframe['_varchar_smaller'] = dataframe['_varchar'].str.slice(0,1)

    # infer SQL properties
    df = dataframe
    df, dtypes, notnull, pk = infer.sql(df)
    _check_dtypes(dtypes)
    _check_dataframe(df, dtypes)
    assert df.columns.isin(notnull).all()
    assert pk=='_tinyint_smaller'

    # infer SQL properties without numeric
    df = dataframe.select_dtypes(['datetime','string'])
    df, dtypes, notnull, pk = infer.sql(df)
    _check_dtypes(dtypes)
    _check_dataframe(df, dtypes)
    assert df.columns.isin(notnull).all()
    assert pk=='_varchar_smaller'


def test_default():

    # setup test data
    dataframe = sample.dataframe
    dataframe['_nvarchar_default'] = None

    # infer SQL properties
    dataframe, dtypes, notnull, pk = infer.sql(dataframe)
    _check_dtypes(dtypes)
    _check_dataframe(dataframe, dtypes)
    assert len(notnull)==0
    assert pk is None
