import pytest
import pandas as pd
from datetime import datetime

from mssql_dataframe import connect
from mssql_dataframe import helpers
from mssql_dataframe import errors
from mssql_dataframe import write
from mssql_dataframe import create


@pytest.fixture(scope="module")
def connection():

    db = connect.SQLServer(database_name='tempdb', server_name='localhost', autocommit=False)
    yield db
    db.connection.close()


def test_safe_sql(connection):

    inputs = ["TableName","##TableName","ColumnName","'; select true; --", "abc[]def", "user's custom name"]
    clean = helpers.safe_sql(connection, inputs)
    assert isinstance(inputs, list)
    assert len(clean)==len(inputs)

    inputs = "SingleString"
    clean = helpers.safe_sql(connection, inputs)
    assert isinstance(inputs, str)
    assert clean=="[SingleString]"

    dataframe = pd.DataFrame(columns=["A","B"])
    clean = helpers.safe_sql(connection, dataframe.columns)
    assert len(clean)==dataframe.shape[1]


def test_where_clause():

    conditions = 'ColumnA >5 AND ColumnB=2 and ColumnANDC IS NOT NULL'
    conditions = helpers.where_clause(conditions)
    assert 1==1


def test_column_spec():

    columns = ['VARCHAR', 'VARCHAR(MAX)', 'VARCHAR(200)', 'VARCHAR(1)', 'INT', 'DECIMAL(5,2)']
    size, dtypes = helpers.column_spec(columns)
    
    assert size==[None, '(MAX)', '(200)', '(1)', None, '(5,2)']
    assert dtypes==['VARCHAR', 'VARCHAR', 'VARCHAR', 'VARCHAR', 'INT', 'DECIMAL']


def test_infer_datatypes(connection):
    
    table_name = '##test_infer_datatypes'

    dataframe = pd.DataFrame({
        '_varchar': [None,'b','c','4','e'],
        '_tinyint': [None,2,3,4,5],
        '_smallint': [256,2,6,4,5],
        '_int': [32768,2,3,4,5],
        '_bigint': [2147483648,2,3,None,5],
        '_float': [1.111111,2,3,4,5],
        '_time': [datetime.now().time()]*5,
        '_datetime': [datetime.now()]*4+[pd.NaT]
    })
    dataframe[['_tinyint','_bigint']] = dataframe[['_tinyint','_bigint']].astype('Int64') 

    columns = {k: 'VARCHAR(100)' for k in dataframe.columns}
    create.table(connection, table_name, columns)
    write.insert(connection, table_name, dataframe)

    dtypes = helpers.infer_datatypes(connection, table_name, column_names=dataframe.columns)
    assert dtypes['_varchar']=="VARCHAR"
    assert dtypes['_tinyint']=="TINYINT"
    assert dtypes['_smallint']=="SMALLINT"
    assert dtypes['_bigint']=="BIGINT"
    assert dtypes['_float']=="FLOAT"
    assert dtypes['_time']=="TIME"
    assert dtypes['_datetime']=="DATETIME"


def test_get_schema(connection):

    table_name = '##test_get_schema'
    with pytest.raises(errors.TableDoesNotExist):
        helpers.get_schema(connection, table_name)

    columns = {"A": "INT"}
    create.table(connection, table_name, columns, primary_key_column="A")
    schema = helpers.get_schema(connection, table_name)

    assert schema.index.name=='column_name'
    assert all(schema.select_dtypes('object').columns==['data_type'])
    assert all(schema.select_dtypes('int64').columns==['max_length', 'precision', 'scale'])
    assert all(schema.select_dtypes('bool').columns==['is_nullable','is_identity', 'is_primary_key'])