from datetime import datetime

import pytest
import pandas as pd

from mssql_dataframe import connect
from mssql_dataframe import create
from mssql_dataframe import helpers


@pytest.fixture(scope="module")
def connection():

    db = connect.SQLServer(database_name='tempdb', server_name='localhost', autocommit=False)
    yield db
    db.connection.close()


@pytest.fixture(scope="module")
def dataframe():
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
    return dataframe


def test_table_column(connection):

    table_name = '##test_table_column'
    columns = {"A": "VARCHAR"}
    create.table(connection, table_name, columns)
    schema = helpers.get_schema(connection, table_name)

    assert len(schema)==1
    assert all(schema.index=='A')
    assert all(schema['data_type']=='varchar')
    assert all(schema['max_length']==1)
    assert all(schema['precision']==0)
    assert all(schema['scale']==0)
    assert all(schema['is_nullable']==True)
    assert all(schema['is_identity']==False)
    assert all(schema['is_primary_key']==False)


def test_table_pk(connection):

    table_name = "##test_table_pk"
    columns = {"A": "TINYINT", "B": "VARCHAR(100)", "C": "DECIMAL(5,2)"}
    primary_key_column = "A"
    not_null = "B"
    create.table(connection, table_name, columns, not_null=not_null, primary_key_column=primary_key_column)
    schema = helpers.get_schema(connection, table_name)

    assert len(schema)==3
    assert all(schema.index==['A','B','C'])
    assert all(schema['data_type']==['tinyint','varchar','decimal'])
    assert all(schema['max_length']==[1,100,5])
    assert all(schema['precision']==[3,0,5])
    assert all(schema['scale']==[0,0,2])
    assert all(schema['is_nullable']==[False, False, True])
    assert all(schema['is_identity']==[False, False, False])
    assert all(schema['is_primary_key']==[True, False, False])


def test_table_sqlpk(connection):

    table_name = '##test_table_sqlpk'
    columns = {"A": "VARCHAR"}
    create.table(connection, table_name, columns, sql_primary_key=True)
    schema = helpers.get_schema(connection, table_name)

    assert len(schema)==2
    assert all(schema.index==['_pk','A'])
    assert all(schema['data_type']==['int','varchar'])
    assert all(schema['max_length']==[4,1])
    assert all(schema['precision']==[10,0])
    assert all(schema['scale']==[0,0])
    assert all(schema['is_nullable']==[False,True])
    assert all(schema['is_identity']==[True,False])
    assert all(schema['is_primary_key']==[True,False])


def test_from_dataframe_simple(connection):

    table_name = '##test_from_dataframe_simple'
    dataframe = pd.DataFrame({"ColumnA": [1]})
    create.from_dataframe(connection, table_name, dataframe)
    schema = helpers.get_schema(connection, table_name)

    assert len(schema)==1
    assert all(schema.index=='ColumnA')
    assert all(schema['data_type']=='bit')
    assert all(schema['max_length']==1)
    assert all(schema['precision']==1)
    assert all(schema['is_nullable']==False)
    assert all(schema['is_identity']==False)
    assert all(schema['is_primary_key']==False)
    assert all(schema['python_type']=='boolean')


def test_from_dataframe_nopk(connection, dataframe):

    table_name = '##test_from_dataframe_nopk'
    create.from_dataframe(connection, table_name, dataframe, primary_key=None)
    schema = helpers.get_schema(connection, table_name)

    assert len(schema)==8
    assert all(schema.index==['_varchar', '_tinyint', '_smallint', '_int', '_bigint', '_float','_time', '_datetime'])
    assert all(schema['data_type']==['varchar', 'tinyint', 'smallint', 'int', 'bigint', 'float', 'time', 'datetime'])
    assert all(schema['max_length']==[1, 1, 2, 4, 8, 8, 5, 8])
    assert all(schema['precision']==[0, 3, 5, 10, 19, 53, 16, 23])
    assert all(schema['scale']==[0, 0, 0, 0, 0, 0, 7, 3])
    assert all(schema['is_nullable']==[True, True, False, False, True, False, False, True])
    assert all(schema['is_identity']==False)
    assert all(schema['is_primary_key']==False)


def test_from_dataframe_sqlpk(connection, dataframe):

    table_name = '##test_from_dataframe_sqlpk'
    create.from_dataframe(connection, table_name, dataframe, primary_key='sql')
    schema = helpers.get_schema(connection, table_name)

    assert len(schema)==9
    assert all(schema.index==['_pk','_varchar', '_tinyint', '_smallint', '_int', '_bigint', '_float','_time', '_datetime'])
    assert all(schema['data_type']==['int','varchar', 'tinyint', 'smallint', 'int', 'bigint', 'float', 'time', 'datetime'])
    assert all(schema['max_length']==[4, 1, 1, 2, 4, 8, 8, 5, 8])
    assert all(schema['precision']==[10, 0, 3, 5, 10, 19, 53, 16, 23])
    assert all(schema['scale']==[0, 0, 0, 0, 0, 0, 0, 7, 3])
    assert all(schema['is_nullable']==[False, True, True, False, False, True, False, False, True])
    assert all(schema['is_identity']==[True, False, False, False, False, False, False, False, False])
    assert all(schema['is_primary_key']==[True, False, False, False, False, False, False, False, False])


def test_from_dataframe_indexpk(connection, dataframe):

    # unamed dataframe index
    table_name = '##test_from_dataframe_indexpk'
    create.from_dataframe(connection, table_name, dataframe, primary_key='index')
    schema = helpers.get_schema(connection, table_name)

    assert len(schema)==9
    assert all(schema.index==['_index','_varchar', '_tinyint', '_smallint', '_int', '_bigint', '_float','_time', '_datetime'])
    assert all(schema['data_type']==['tinyint','varchar', 'tinyint', 'smallint', 'int', 'bigint', 'float', 'time', 'datetime'])
    assert all(schema['max_length']==[1, 1, 1, 2, 4, 8, 8, 5, 8])
    assert all(schema['precision']==[3, 0, 3, 5, 10, 19, 53, 16, 23])
    assert all(schema['scale']==[0, 0, 0, 0, 0, 0, 0, 7, 3])
    assert all(schema['is_nullable']==[False, True, True, False, False, True, False, False, True])
    assert all(schema['is_identity']==False)
    assert all(schema['is_primary_key']==[True, False, False, False, False, False, False, False, False])

    # named dataframe index
    table_name = '##DataFrameIndexPKNamed'
    dataframe.index.name = 'NamedIndex'
    create.from_dataframe(connection, table_name, dataframe, primary_key='index')
    schema = helpers.get_schema(connection, table_name)

    assert len(schema)==9
    assert all(schema.index==['NamedIndex','_varchar', '_tinyint', '_smallint', '_int', '_bigint', '_float','_time', '_datetime'])
    assert all(schema['data_type']==['tinyint','varchar', 'tinyint', 'smallint', 'int', 'bigint', 'float', 'time', 'datetime'])
    assert all(schema['max_length']==[1, 1, 1, 2, 4, 8, 8, 5, 8])
    assert all(schema['precision']==[3, 0, 3, 5, 10, 19, 53, 16, 23])
    assert all(schema['scale']==[0, 0, 0, 0, 0, 0, 0, 7, 3])
    assert all(schema['is_nullable']==[False, True, True, False, False, True, False, False, True])
    assert all(schema['is_identity']==False)
    assert all(schema['is_primary_key']==[True, False, False, False, False, False, False, False, False])


def test_from_dataframe_inferpk(connection):

    # integer primary key
    dataframe = pd.DataFrame({
        '_varchar1': ['a','b','c','d','e'],
        '_varchar2': ['aa','b','c','d','e'],
        '_tinyint': [None, 2, 3, 4, 5],
        '_smallint': [265, 2, 6, 4, 5],
        '_int': [32768, 2, 3, 4, 5],
        '_float1': [1.1111, 2, 3, 4, 5],
        '_float2': [1.1111, 2, 3, 4, 6]
    })
    table_name = '##test_from_dataframe_inferpk_integer'
    create.from_dataframe(connection, table_name, dataframe, primary_key='infer')
    schema = helpers.get_schema(connection, table_name)
    assert schema.at['_smallint','is_primary_key']

    # float primary key
    dataframe = pd.DataFrame({
        '_varchar1': ['a','b','c','d','e'],
        '_varchar2': ['aa','b','c','d','e'],
        '_tinyint': [None, 2, 3, 4, 5],
        '_float1': [1.1111, 2, 3, 4, 5],
        '_float2': [1.1111, 2, 3, 4, 6]
    })
    table_name = '##test_from_dataframe_inferpk_float'
    create.from_dataframe(connection, table_name, dataframe, primary_key='infer')
    schema = helpers.get_schema(connection, table_name)
    assert schema.at['_float1','is_primary_key']

    # string primary key
    dataframe = pd.DataFrame({
        '_varchar1': ['a','b','c','d','e'],
        '_varchar2': ['aa','b','c','d','e'],
    })
    table_name = '##test_from_dataframe_inferpk_string'
    create.from_dataframe(connection, table_name, dataframe, primary_key='infer')
    schema = helpers.get_schema(connection, table_name)
    assert schema.at['_varchar1','is_primary_key']

    # uninferrable primary key
    dataframe = pd.DataFrame({
        '_varchar1': [None,'b','c','d','e'],
        '_varchar2': [None,'b','c','d','e'],
    })
    table_name = '##test_from_dataframe_inferpk_uninferrable'
    create.from_dataframe(connection, table_name, dataframe, primary_key='infer')
    schema = helpers.get_schema(connection, table_name)
    assert all(schema['is_primary_key']==False)

