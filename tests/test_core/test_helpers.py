from datetime import datetime
import warnings

import pytest
import pandas as pd

from mssql_dataframe import connect
from mssql_dataframe.core import errors, helpers, create, write, modify


class package:
    def __init__(self, connection):
        self.connection = connection
        self.create = create.create(connection)
        self.write = write.write(connection, adjust_sql_objects=False)
        self.modify = modify.modify(connection)

@pytest.fixture(scope="module")
def sql():
    db = connect.connect(database_name='tempdb', server_name='localhost', autocommit=False)
    yield package(db)
    db.connection.close()


def test_execute(sql):

    with pytest.raises(errors.SQLGeneral):
        helpers.execute(sql.connection, statement='error')


def test_safe_sql(sql):

    # list of values
    inputs = ["TableName","##TableName","ColumnName","'; select true; --", "abc[]def", "user's custom name"]
    clean = helpers.safe_sql(sql.connection, inputs)
    assert isinstance(inputs, list)
    assert len(clean)==len(inputs)

    # single string
    inputs = "SingleString"
    clean = helpers.safe_sql(sql.connection, inputs)
    assert isinstance(inputs, str)

    # dataframe columns
    dataframe = pd.DataFrame(columns=["A","B"])
    clean = helpers.safe_sql(sql.connection, dataframe.columns)
    assert len(clean)==dataframe.shape[1]

    # schema specification list
    inputs = ["test.dbo.table","tempdb..##table"]
    clean = helpers.safe_sql(sql.connection, inputs)
    assert len(clean)==len(inputs)

    # schema specification single string
    inputs = "test.dbo.table"
    clean = helpers.safe_sql(sql.connection, inputs)
    assert isinstance(clean, str)

    # value that is too long
    with pytest.raises(errors.SQLInvalidLengthObjectName):
        helpers.safe_sql(sql.connection, inputs='a'*1000)


def test_where_clause(sql):

    where = 'ColumnA >5 AND ColumnB=2 and ColumnANDC IS NOT NULL'
    where_statement, where_args = helpers.where_clause(sql.connection, where)
    assert where_statement=='WHERE [ColumnA] > ? AND [ColumnB] = ? and [ColumnANDC] IS NOT NULL'
    assert where_args==['5','2']

    where = 'ColumnB>4 AND ColumnC IS NOT NULL OR ColumnD IS NULL'
    where_statement, where_args = helpers.where_clause(sql.connection, where)
    assert where_statement=='WHERE [ColumnB] > ? AND [ColumnC] IS NOT NULL OR [ColumnD] IS NULL'
    assert where_args==['4']

    conditions = 'no operator present'
    with pytest.raises(errors.SQLInvalidSyntax):
        helpers.where_clause(sql.connection, conditions)


def test_column_spec():

    columns = ['VARCHAR', 'VARCHAR(MAX)', 'VARCHAR(200)', 'VARCHAR(1)', 'INT', 'DECIMAL(5,2)']
    size, dtypes = helpers.column_spec(columns)
    
    assert size==[None, '(MAX)', '(200)', '(1)', None, '(5,2)']
    assert dtypes==['VARCHAR', 'VARCHAR', 'VARCHAR', 'VARCHAR', 'INT', 'DECIMAL']


def test_infer_datatypes_simple(sql):
    
    table_name = '##test_infer_datatypes_simple'

    dataframe = pd.DataFrame({'_tinyint': [1]})

    dtypes = helpers.infer_datatypes(sql.connection, table_name, dataframe)
    assert dtypes['_tinyint']=='tinyint'


def test_infer_datatypes(sql):
    
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

    dtypes = helpers.infer_datatypes(sql.connection, table_name, dataframe)
    assert dtypes['_varchar']=="varchar(1)"
    assert dtypes['_bit']=="bit"
    assert dtypes['_tinyint']=="tinyint"
    assert dtypes['_smallint']=="smallint"
    assert dtypes['_bigint']=="bigint"
    assert dtypes['_float']=="float"
    assert dtypes['_time']=="time"
    assert dtypes['_datetime']=="datetime"


def test_infer_datatypes_small_sample(sql):

    table_name = '##test_table_datatypes_small_sample'

    dataframe = pd.DataFrame({
    '_tinyint': list(range(0,5,1)),
    '_varchar': ['aaaaa','bbbb','ccc','dd','e']
    })

    dtypes = helpers.infer_datatypes(sql.connection, table_name, dataframe, row_count=0)

    assert dtypes['_tinyint']=="tinyint"
    assert dtypes['_varchar']=="varchar(5)"
    

def test_get_schema(sql):

    table_name = '##test_get_schema'

    columns = {"_varchar": "VARCHAR", "_bit": "BIT", "_tinyint": "TINYINT", "_smallint": "SMALLINT", "_int": "INT", "_bigint": "BIGINT",
    "_float": "FLOAT", "_time": "TIME", "_datetime": "DATETIME"}
    sql.create.table(table_name, columns)
    schema = helpers.get_schema(sql.connection, table_name)

    assert schema.index.name=='column_name'
    assert all(schema.select_dtypes('object').columns==['data_type','python_type'])
    assert all(schema.select_dtypes('int64').columns==['max_length', 'precision', 'scale'])
    assert all(schema.select_dtypes('bool').columns==['is_nullable','is_identity', 'is_primary_key'])


def test_get_schema_errors(sql):

    # table does not exist
    table_name = '##test_get_schema_errors'
    with pytest.raises(errors.SQLTableDoesNotExist):
        helpers.get_schema(sql.connection, table_name)


def test_get_schema_undefined(sql):

    table_name = '##test_get_schema_undefined'
    columns = {"_geography": "GEOGRAPHY", "_hierarchyid": "HIERARCHYID"}
    sql.create.table(table_name, columns)

    with warnings.catch_warnings(record=True) as warn:
        schema = helpers.get_schema(sql.connection, table_name)
        assert len(warn)==1
        assert isinstance(warn[-1].message, errors.DataframeUndefinedBestType)
        assert "['_geography', '_hierarchyid']" in str(warn[-1].message)
        assert all(schema['python_type']=='str')


def test_read_query_undefined_type(sql):

    table_name = '##test_read_query_undefined_type'
    columns = {"_geography": "GEOGRAPHY", "_datetimeoffset": "DATETIMEOFFSET(4)"}
    sql.create.table(table_name, columns)

    geography = "geography::STGeomFromText('LINESTRING(-122.360 47.656, -122.343 47.656)', 4326)"
    datetimeoffset = "'12-10-25 12:32:10 +01:00'"
    statement = "INSERT INTO {table_name} VALUES({geography},{datetimeoffset})"
    sql.connection.connection.cursor().execute(statement.format(
        table_name=table_name,
        geography=geography,
        datetimeoffset=datetimeoffset
    ))

    with warnings.catch_warnings(record=True) as warn:
        dataframe = helpers.read_query(sql.connection, "SELECT * FROM {table_name}".format(table_name=table_name))
        assert len(warn)==1
        assert issubclass(warn[-1].category, UserWarning)
        assert "['_geography', '_datetimeoffset']" in str(warn[-1].message)
        assert len(dataframe)==1
        assert all(dataframe.columns==['_geography', '_datetimeoffset'])


def test_flatten_schema():

    schema = pd.DataFrame.from_dict(
        {
        '_index': ['bit', 1, 1, 0, False, False, True],
        'ColumnA': ['tinyint', 1, 3, 0, False, False, False],
        'ColumnB': ['varchar', 1, 0, 0, False, False, False],
        'ColumnC': ['decimal', 5, 5, 2, False, False, False]
        }, orient='index', columns = ['data_type','max_length','precision','scale','is_nullable','is_identity','is_primary_key']
    )
    schema.index.name = 'column_name'

    columns, not_null, primary_key_column, sql_primary_key = helpers.flatten_schema(schema)
    
    assert columns=={'_index': 'bit', 'ColumnA': 'tinyint', 'ColumnB': 'varchar(1)', 'ColumnC': 'decimal(5,2)'}
    assert not_null==['_index', 'ColumnA', 'ColumnB', 'ColumnC']
    assert primary_key_column=='_index'
    assert sql_primary_key==False


def test_get_pk_details(sql):

    table_name = "##test_get_pk_details"
    columns = {"_pk": "TINYINT", "A": 'VARCHAR(1)'}
    primary_key_column = "_pk"
    sql.create.table(table_name, columns, primary_key_column = primary_key_column)
    primary_key_name, primary_key_column = helpers.get_pk_details(sql.connection, table_name)
    assert isinstance(primary_key_name, str)
    assert primary_key_column=='_pk'

    table_name = "##test_get_pk_name_two_columns"
    columns = {"A": "INT", "B": "BIGINT", "C": "BIGINT", "D": "BIGINT"}
    sql.create.table(table_name, columns, not_null=["A","B"])
    pk_columns = ['A','B']
    pk_name= '_pk_1'
    sql.modify.primary_key(table_name, modify='add', columns=pk_columns, primary_key_name = pk_name)
    primary_key_name, primary_key_column = helpers.get_pk_details(sql.connection, table_name)
    assert primary_key_name==pk_name
    assert primary_key_column==pk_columns

    table_name = "##test_get_pk_error"
    columns = {"A": "INT"}
    sql.create.table(table_name, columns)
    with pytest.raises(errors.SQLUndefinedPrimaryKey):
        _ = helpers.get_pk_details(sql.connection, table_name)