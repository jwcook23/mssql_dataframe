import pandas as pd

from mssql_dataframe.core import infer
from . import sample


def test_dtypes():

    # setup test data
    dataframe = sample.dataframe
    na = dataframe.isna()
    dataframe = sample.dataframe.astype('str')
    dataframe[na] = None
    dataframe['_time'] = dataframe['_time'].str.replace('0 days ','')

    # infer SQL properties
    dataframe, dtypes, notnull = infer.sql(dataframe)

    # insure dataframe dtypes are correct
    expected = dataframe.dtypes.apply(lambda x: x.name)
    expected.name = 'pandas'
    expected = expected.reset_index()
    expected = expected.sort_values(by='column_name', ignore_index=True)
    actual = dtypes['pandas'].reset_index()
    actual = actual.sort_values(by='column_name', ignore_index=True)
    assert actual.equals(expected)

    # insure sql dtypes are correct
    expected = dtypes['sql'].reset_index()
    expected['actual'] = expected['column_name'].str.slice(1)
    assert (expected['sql']==expected['actual']).all()

    # insure all columns are nullable
    assert len(notnull)==0


def test_pk():

    # setup test data
    dataframe = sample.dataframe
    dataframe = dataframe[dataframe.notna().all(axis='columns')]
    dataframe['_tinyint_smaller'] = pd.Series(range(0,len(dataframe)), dtype='UInt8')
    dataframe['_varchar_smaller'] = dataframe['_varchar'].str.slice(0,1)

    # infer SQL properties
    dataframe, dtypes, notnull, pk = infer.sql(dataframe)
    assert 1==2 # test for dataframe (insure types are unchanged) and dtypes, make testing function out of contents in test_dtypes()
    assert dataframe.columns.isin(notnull).all()
    assert pk=='_tinyint_smaller'

    # infer SQL properties without numeric
    dataframe, dtypes, notnull, pk = infer.sql(dataframe.select_dtypes(['datetime','string']))
    assert 1==2 # test for dataframe (insure types are unchanged) and dtypes, make testing function out of contents in test_dtypes()
    assert dataframe.columns.isin(notnull).all()
    assert pk=='_varchar_smaller'

    assert 1==2