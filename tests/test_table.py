from datetime import datetime

import pytest
import pandas as pd

from mssql_dataframe import connect
from mssql_dataframe import table


@pytest.fixture(scope="module")
def connection():
    db = connect.SQLServer(database_name='tempdb', server_name='localhost', autocommit=False)
    sql = table.table(db)
    yield sql
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


def verify_schema(connection, table_name, data, is_identity: str = None):
    
    # prevent pytest showing traceback
    __tracebackhide__ = True

    # # columns with any na values are nullable
    # is_nullable = data.dataframe.isna().any()

    # # schema for auto created primary keys
    # data.sql_dtype['_sqlpk'] = 'int'
    # data.sql_dtype['_indexpk'] = 'tinyint'
    # is_nullable['_sqlpk'] = False
    # is_nullable['_indexpk'] = False

    # # max length
    # max_length =  {'varchar': 255}

    # # get schema from SQL
    # # columns = 'name, TYPE_NAME(SYSTEM_TYPE_ID) AS data_type, max_length, precision, scale, is_nullable, is_identity'
    # # statement = "SELECT "+columns+" FROM  SYS.COLUMNS WHERE OBJECT_ID = OBJECT_ID('"+table_name+"')"

    # statement = '''
    # SELECT
    #     _columns.name,
    #     TYPE_NAME(SYSTEM_TYPE_ID) AS data_type, 
    #     _columns.max_length, 
    #     _columns.precision, 
    #     _columns.scale, 
    #     _columns.is_nullable, 
    #     _columns.is_identity,
    #     _index.is_primary_key
    # FROM sys.columns AS _columns
    # LEFT JOIN sys.index_columns AS _ic
    #     ON _ic.object_id = _columns.object_id AND  _ic.column_id = _columns.column_id
    # LEFT JOIN sys.indexes AS _index
    #     ON _index.object_id = _ic.object_id AND _index.index_id = _ic.index_id
    # WHERE _columns.object_ID = OBJECT_ID('{}')
    # '''
    # statement = statement.format(table_name)


    # result = connection.cursor.execute(statement).fetchall()
    # result = [list(x) for x in result]

    # columns = [col[0] for col in connection.cursor.description]

    # result = pd.DataFrame(result, columns=columns).set_index('name')
    # result = result.T.to_dict()

    # for col, schema in result.items():

    #     # datatype
    #     expected = data.sql_dtype[col]
    #     actual = schema['data_type']
    #     if actual!=expected:
    #         msg = 'SQL {} error; column "{}"; expected: "{}"; actual: "{}"'
    #         pytest.fail(msg.format('data_type', col, expected, actual))
    #     if schema['data_type']=='varchar':
    #         expected = max_length['varchar']
    #         actual = schema['max_length']
    #         if actual!=expected:
    #             msg = 'SQL {} error; column "{}"; expected: "{}"; actual: "{}"'
    #             pytest.fail(msg.format('max_length', col, expected, actual))

    #     # is_nullable
    #     expected = is_nullable[col]
    #     actual = schema['is_nullable']
    #     if actual!=expected:
    #         msg = 'SQL {} error; column "{}"; expected: "{}"; actual: "{}"'
    #         pytest.fail(msg.format('is_nullable', col, expected, actual))

    #     # is_identity (pk)
    #     if is_identity is not None and col==is_identity:
    #         expected = True
    #         actual = schema['is_identity']
    #         if actual!=expected:
    #             msg = 'SQL {} error; column "{}"; expected: "{}"; actual: "{}"'
    #             pytest.fail(msg.format('is_identity', col, expected, actual))

    # return True


def test__column_spec(connection):

    columns = ['VARCHAR', 'VARCHAR(MAX)', 'VARCHAR(200)', 'INT', 'DECIMAL(5,2)']
    size, dtypes = connection._table__column_spec(columns)

    assert size==[None, '(MAX)', '(200)', None, '(5,2)']
    assert dtypes==['VARCHAR', 'VARCHAR', 'VARCHAR', 'INT', 'DECIMAL']


def test_get_schema_nonexistant(connection):

    table_name = '##NonExistantTable'
    with pytest.raises(table.TableDoesNotExist):
        connection.get_schema(table_name)


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


def test_create_table_sql_sqlprimarykey(connection):

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


def test_from_dataframe_indexpk_unnamed(connection, dataframe):

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


def test_from_dataframe_indexpk_named(connection, dataframe):

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


def test_from_dataframe_inferpk_integer(connection):

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


def test_from_dataframe_inferpk_float(connection):

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


def test_from_dataframe_inferpk_string(connection):

    dataframe = pd.DataFrame({
        '_varchar1': ['a','b','c','d','e'],
        '_varchar2': ['aa','b','c','d','e'],
    })

    table_name = '##DataFrameInferPKString'
    connection.from_dataframe(table_name, dataframe, primary_key='infer')
    schema = connection.get_schema(table_name)

    assert schema.at['_varchar1','is_primary_key']


def test_from_dataframe_inferpk_uninferrable(connection):

    dataframe = pd.DataFrame({
        '_varchar1': [None,'b','c','d','e'],
        '_varchar2': [None,'b','c','d','e'],
    })

    table_name = '##DataFrameInferPKUninferrable'
    connection.from_dataframe(table_name, dataframe, primary_key='infer')
    schema = connection.get_schema(table_name)

    assert all(schema['is_primary_key']==False)


def test_modify_column_drop(connection):

    table_name = '##ModifyColumnDrop'
    columns = {"A": "VARCHAR", "B": "VARCHAR"}
    connection.create_table(table_name, columns)
    
    connection.modify_column(table_name, modify='drop', column_name='B')
    schema = connection.get_schema(table_name)
    assert 'B' not in schema.index


def test_modify_column_add(connection):

    table_name = '##ModifyColumnAdd'
    columns = {"A": "VARCHAR"}
    connection.create_table(table_name, columns)

    connection.modify_column(table_name, modify='add', column_name='B', data_type='VARCHAR(20)')
    schema = connection.get_schema(table_name)
    assert 'B' in schema.index
    assert schema.at['B','data_type']=='varchar'
    assert schema.at['B','max_length']==20

    connection.modify_column(table_name, modify='add', column_name='C', data_type='BIGINT')
    schema = connection.get_schema(table_name)
    assert 'C' in schema.index
    assert schema.at['C','data_type']=='bigint' 


def test_modify_column_alter(connection):

    table_name = '##ModifyColumnAlter'
    columns = {"A": "VARCHAR(10)", "B": "BIGINT", "C": "BIGINT", "D": "BIGINT"}
    connection.create_table(table_name, columns)

    connection.modify_column(table_name, modify='alter', column_name='B', data_type='INT')
    schema = connection.get_schema(table_name)
    assert 'B' in schema.index
    assert schema.at['B','data_type']=='int'
    assert schema.at['B', 'is_nullable']==True

    connection.modify_column(table_name, modify='alter', column_name='C', data_type='INT', not_null=True)
    schema = connection.get_schema(table_name)
    assert 'C' in schema.index
    assert schema.at['C','data_type']=='int'
    assert schema.at['C', 'is_nullable']==False