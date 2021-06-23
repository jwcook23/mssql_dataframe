from datetime import datetime

import pytest
import pandas as pd

from mssql_dataframe import connect


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


def test_create_table_column(connection):

    table_name = '##SingleColumnTable'
    columns = {"A": "VARCHAR"}
    connection.create_table(table_name, columns)
    schema = connection.get_schema(table_name)

    assert len(schema)==1
    assert all(schema.index=='A')
    assert all(schema['data_type']=='varchar')
    assert all(schema['max_length']==1)
    assert all(schema['precision']==0)
    assert all(schema['scale']==0)
    assert all(schema['is_nullable']==True)
    assert all(schema['is_identity']==False)
    assert all(schema['is_primary_key']==False)


def test_create_table_primarykey(connection):

    table_name = "##PrimaryKey"
    columns = {"A": "TINYINT", "B": "VARCHAR(100)", "C": "DECIMAL(5,2)"}
    primary_key_column = "A"
    not_null = "B"
    connection.create_table(table_name, columns, not_null=not_null, primary_key_column=primary_key_column)
    schema = connection.get_schema(table_name)

    assert len(schema)==3
    assert all(schema.index==['A','B','C'])
    assert all(schema['data_type']==['tinyint','varchar','decimal'])
    assert all(schema['max_length']==[1,100,5])
    assert all(schema['precision']==[3,0,5])
    assert all(schema['scale']==[0,0,2])
    assert all(schema['is_nullable']==[False, False, True])
    assert all(schema['is_identity']==[False, False, False])
    assert all(schema['is_primary_key']==[True, False, False])


def test_create_table_sqlprimarykey(connection):

    table_name = '##SQLPrimaryKey'
    columns = {"A": "VARCHAR"}
    connection.create_table(table_name, columns, sql_primary_key=True)
    schema = connection.get_schema(table_name)

    assert len(schema)==2
    assert all(schema.index==['_pk','A'])
    assert all(schema['data_type']==['int','varchar'])
    assert all(schema['max_length']==[4,1])
    assert all(schema['precision']==[10,0])
    assert all(schema['scale']==[0,0])
    assert all(schema['is_nullable']==[False,True])
    assert all(schema['is_identity']==[True,False])
    assert all(schema['is_primary_key']==[True,False])


def test_from_dataframe_nopk(connection, dataframe):

    table_name = '##DataFrameNoPK'
    connection.from_dataframe(table_name, dataframe, primary_key=None)
    schema = connection.get_schema(table_name)

    assert len(schema)==8
    assert all(schema.index==['_varchar', '_tinyint', '_smallint', '_int', '_bigint', '_float','_time', '_datetime'])
    assert all(schema['data_type']==['varchar', 'tinyint', 'smallint', 'int', 'bigint', 'float', 'time', 'datetime'])
    assert all(schema['max_length']==[255, 1, 2, 4, 8, 8, 5, 8])
    assert all(schema['precision']==[0, 3, 5, 10, 19, 53, 16, 23])
    assert all(schema['scale']==[0, 0, 0, 0, 0, 0, 7, 3])
    assert all(schema['is_nullable']==[True, True, False, False, True, False, False, True])
    assert all(schema['is_identity']==False)
    assert all(schema['is_primary_key']==False)


def test_from_dataframe_sqlpk(connection, dataframe):

    table_name = '##DataFrameSQLPK'
    connection.from_dataframe(table_name, dataframe, primary_key='sql')
    schema = connection.get_schema(table_name)

    assert len(schema)==9
    assert all(schema.index==['_pk','_varchar', '_tinyint', '_smallint', '_int', '_bigint', '_float','_time', '_datetime'])
    assert all(schema['data_type']==['int','varchar', 'tinyint', 'smallint', 'int', 'bigint', 'float', 'time', 'datetime'])
    assert all(schema['max_length']==[4, 255, 1, 2, 4, 8, 8, 5, 8])
    assert all(schema['precision']==[10, 0, 3, 5, 10, 19, 53, 16, 23])
    assert all(schema['scale']==[0, 0, 0, 0, 0, 0, 0, 7, 3])
    assert all(schema['is_nullable']==[False, True, True, False, False, True, False, False, True])
    assert all(schema['is_identity']==[True, False, False, False, False, False, False, False, False])
    assert all(schema['is_primary_key']==[True, False, False, False, False, False, False, False, False])


def test_from_dataframe_indexpk(connection, dataframe):

    # unamed dataframe index
    table_name = '##DataFrameIndexPKUnnamed'
    connection.from_dataframe(table_name, dataframe, primary_key='index')
    schema = connection.get_schema(table_name)

    assert len(schema)==9
    assert all(schema.index==['_index','_varchar', '_tinyint', '_smallint', '_int', '_bigint', '_float','_time', '_datetime'])
    assert all(schema['data_type']==['tinyint','varchar', 'tinyint', 'smallint', 'int', 'bigint', 'float', 'time', 'datetime'])
    assert all(schema['max_length']==[1, 255, 1, 2, 4, 8, 8, 5, 8])
    assert all(schema['precision']==[3, 0, 3, 5, 10, 19, 53, 16, 23])
    assert all(schema['scale']==[0, 0, 0, 0, 0, 0, 0, 7, 3])
    assert all(schema['is_nullable']==[False, True, True, False, False, True, False, False, True])
    assert all(schema['is_identity']==False)
    assert all(schema['is_primary_key']==[True, False, False, False, False, False, False, False, False])

    # named dataframe index
    table_name = '##DataFrameIndexPKNamed'
    dataframe.index.name = 'NamedIndex'
    connection.from_dataframe(table_name, dataframe, primary_key='index')
    schema = connection.get_schema(table_name)

    assert len(schema)==9
    assert all(schema.index==['NamedIndex','_varchar', '_tinyint', '_smallint', '_int', '_bigint', '_float','_time', '_datetime'])
    assert all(schema['data_type']==['tinyint','varchar', 'tinyint', 'smallint', 'int', 'bigint', 'float', 'time', 'datetime'])
    assert all(schema['max_length']==[1, 255, 1, 2, 4, 8, 8, 5, 8])
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
    table_name = '##DataFrameInferPKInteger'
    connection.from_dataframe(table_name, dataframe, primary_key='infer')
    schema = connection.get_schema(table_name)
    assert schema.at['_smallint','is_primary_key']

    # float primary key
    dataframe = pd.DataFrame({
        '_varchar1': ['a','b','c','d','e'],
        '_varchar2': ['aa','b','c','d','e'],
        '_tinyint': [None, 2, 3, 4, 5],
        '_float1': [1.1111, 2, 3, 4, 5],
        '_float2': [1.1111, 2, 3, 4, 6]
    })
    table_name = '##DataFrameInferPKFloat'
    connection.from_dataframe(table_name, dataframe, primary_key='infer')
    schema = connection.get_schema(table_name)
    assert schema.at['_float1','is_primary_key']

    # string primary key
    dataframe = pd.DataFrame({
        '_varchar1': ['a','b','c','d','e'],
        '_varchar2': ['aa','b','c','d','e'],
    })
    table_name = '##DataFrameInferPKString'
    connection.from_dataframe(table_name, dataframe, primary_key='infer')
    schema = connection.get_schema(table_name)
    assert schema.at['_varchar1','is_primary_key']

    # uninferrable primary key
    dataframe = pd.DataFrame({
        '_varchar1': [None,'b','c','d','e'],
        '_varchar2': [None,'b','c','d','e'],
    })
    table_name = '##DataFrameInferPKUninferrable'
    connection.from_dataframe(table_name, dataframe, primary_key='infer')
    schema = connection.get_schema(table_name)
    assert all(schema['is_primary_key']==False)


def test_modify_column_drop(connection):

    modify = 'drop'
    table_name = '##ModifyColumnDrop'
    columns = {"A": "VARCHAR", "B": "VARCHAR"}
    connection.create_table(table_name, columns)
    
    connection.modify_column(table_name, modify=modify, column_name='B')
    schema = connection.get_schema(table_name)
    assert 'B' not in schema.index


def test_modify_column_add(connection):

    modify = 'add'
    table_name = '##ModifyColumnAdd'
    columns = {"A": "VARCHAR"}
    connection.create_table(table_name, columns)

    connection.modify_column(table_name, modify=modify, column_name='B', data_type='VARCHAR(20)')
    schema = connection.get_schema(table_name)
    assert 'B' in schema.index
    assert schema.at['B','data_type']=='varchar'
    assert schema.at['B','max_length']==20

    connection.modify_column(table_name, modify=modify, column_name='C', data_type='BIGINT')
    schema = connection.get_schema(table_name)
    assert 'C' in schema.index
    assert schema.at['C','data_type']=='bigint' 


def test_modify_column_alter(connection):

    modify = 'alter'
    table_name = '##ModifyColumnAlter'
    columns = {"A": "VARCHAR(10)", "B": "BIGINT", "C": "BIGINT", "D": "BIGINT"}
    connection.create_table(table_name, columns)

    connection.modify_column(table_name, modify=modify, column_name='B', data_type='INT')
    schema = connection.get_schema(table_name)
    assert 'B' in schema.index
    assert schema.at['B','data_type']=='int'
    assert schema.at['B', 'is_nullable']==True

    connection.modify_column(table_name, modify=modify, column_name='C', data_type='INT', not_null=True)
    schema = connection.get_schema(table_name)
    assert 'C' in schema.index
    assert schema.at['C','data_type']=='int'
    assert schema.at['C', 'is_nullable']==False


def test_modify_primary_key(connection):

    table_name = '##ModifyPrimaryKey'
    columns = {"A": "INT", "B": "BIGINT", "C": "BIGINT", "D": "BIGINT"}
    connection.create_table(table_name, columns, not_null=["A","B"])

    connection.modify_primary_key(table_name, modify='add', columns=['A','B'], primary_key_name = '_pk_1')
    schema = connection.get_schema(table_name)
    assert schema.at['A','is_primary_key']==True

    connection.modify_primary_key(table_name, modify='drop', columns=['A','B'],  primary_key_name = '_pk_1')
    schema = connection.get_schema(table_name)
    assert schema.at['A','is_primary_key']==False