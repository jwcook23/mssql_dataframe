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

    # list of values
    inputs = ["TableName","##TableName","ColumnName","'; select true; --", "abc[]def", "user's custom name"]
    clean = helpers.safe_sql(connection, inputs)
    assert isinstance(inputs, list)
    assert len(clean)==len(inputs)

    # single string
    inputs = "SingleString"
    clean = helpers.safe_sql(connection, inputs)
    assert isinstance(inputs, str)
    assert clean=="[SingleString]"

    # dataframe columns
    dataframe = pd.DataFrame(columns=["A","B"])
    clean = helpers.safe_sql(connection, dataframe.columns)
    assert len(clean)==dataframe.shape[1]

    # value that is too long
    with pytest.raises(errors.GeneralError):
        helpers.safe_sql(connection, inputs='a'*1000)


def test_where_clause(connection):

    where = 'ColumnA >5 AND ColumnB=2 and ColumnANDC IS NOT NULL'
    where_statement, where_args = helpers.where_clause(connection, where)
    assert where_statement=='WHERE [ColumnA] > ? AND [ColumnB] = ? and [ColumnANDC] IS NOT NULL'
    assert where_args==['5','2']

    where = 'ColumnB>4 AND ColumnC IS NOT NULL OR ColumnD IS NULL'
    where_statement, where_args = helpers.where_clause(connection, where)
    assert where_statement=='WHERE [ColumnB] > ? AND [ColumnC] IS NOT NULL OR [ColumnD] IS NULL'
    assert where_args==['4']

    conditions = 'no operator present'
    with pytest.raises(errors.InvalidSyntax):
        helpers.where_clause(connection, conditions)


def test_column_spec():

    columns = ['VARCHAR', 'VARCHAR(MAX)', 'VARCHAR(200)', 'VARCHAR(1)', 'INT', 'DECIMAL(5,2)']
    size, dtypes = helpers.column_spec(columns)
    
    assert size==[None, '(MAX)', '(200)', '(1)', None, '(5,2)']
    assert dtypes==['VARCHAR', 'VARCHAR', 'VARCHAR', 'VARCHAR', 'INT', 'DECIMAL']


def test_infer_datatypes_simple(connection):
    
    table_name = '##test_infer_datatypes_simple'

    dataframe = pd.DataFrame({'_bit': [1]})

    dtypes = helpers.infer_datatypes(connection, table_name, dataframe)
    assert dtypes['_bit']=='BIT'


def test_infer_datatypes(connection):
    
    table_name = '##test_infer_datatypes'

    dataframe = pd.DataFrame({
        '_varchar': [None,'b','c','4','e'],
        '_bit': [0,1,1,0,None],
        '_tinyint': [None,2,3,4,5],
        '_smallint': [256,2,6,4,5],
        '_int': [32768,2,3,4,5],
        '_bigint': [2147483648,2,3,None,5],
        '_float': [1.111111,2,3,4,5],
        '_time': [datetime.now().time()]*5,
        '_datetime': [datetime.now()]*4+[pd.NaT]
    })
    dataframe[['_bit','_tinyint','_bigint']] = dataframe[['_bit','_tinyint','_bigint']].astype('Int64') 

    dtypes = helpers.infer_datatypes(connection, table_name, dataframe)
    assert dtypes['_varchar']=="VARCHAR(1)"
    assert dtypes['_bit']=="BIT"
    assert dtypes['_tinyint']=="TINYINT"
    assert dtypes['_smallint']=="SMALLINT"
    assert dtypes['_bigint']=="BIGINT"
    assert dtypes['_float']=="FLOAT"
    assert dtypes['_time']=="TIME"
    assert dtypes['_datetime']=="DATETIME"


def test_get_schema(connection):

    table_name = '##test_get_schema'

    columns = {"_varchar": "VARCHAR", "_bit": "BIT", "_tinyint": "TINYINT", "_smallint": "SMALLINT", "_int": "INT", "_bigint": "BIGINT",
    "_float": "FLOAT", "_time": "TIME", "_datetime": "DATETIME"}
    create.table(connection, table_name, columns)
    schema = helpers.get_schema(connection, table_name)

    assert schema.index.name=='column_name'
    assert all(schema.select_dtypes('object').columns==['data_type','python_type'])
    assert all(schema.select_dtypes('int64').columns==['max_length', 'precision', 'scale'])
    assert all(schema.select_dtypes('bool').columns==['is_nullable','is_identity', 'is_primary_key'])


def test_get_schema_errors(connection):

    # table does not exist
    table_name = '##test_get_schema_errors'
    with pytest.raises(errors.TableDoesNotExist):
        helpers.get_schema(connection, table_name)
    
    columns = {"_error": "SMALLDATETIME"}
    create.table(connection, table_name, columns)
    with pytest.raises(errors.UndefinedPythonDataType):
        helpers.get_schema(connection, table_name)