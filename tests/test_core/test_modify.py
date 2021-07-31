import pytest

from mssql_dataframe import connect
from mssql_dataframe.core import helpers, create, modify


class package:
    def __init__(self, connection):
        self.connection = connection
        self.create = create.create(connection)
        self.modify = modify.modify(connection)

@pytest.fixture(scope="module")
def sql():
    db = connect.connect(database_name='tempdb', server_name='localhost')
    yield package(db)
    db.connection.close()


def test_column_input_error(sql):

    table_name = '##column_input_error'
    columns = {"A": "VARCHAR", "B": "VARCHAR"}
    sql.create.table(table_name, columns)
    
    with pytest.raises(ValueError) as error:
        sql.modify.column(table_name, modify='delete', column_name='B')
    assert 'modify must be one of: ' in str(error)


def test_column_drop(sql):

    table_name = '##column_drop'
    columns = {"A": "VARCHAR", "B": "VARCHAR"}
    sql.create.table(table_name, columns)
    
    sql.modify.column(table_name, modify='drop', column_name='B')
    schema = helpers.get_schema(sql.connection, table_name)
    assert 'B' not in schema.index


def test_column_add(sql):

    table_name = '##test_column_add'
    columns = {"A": "VARCHAR"}
    sql.create.table(table_name, columns)

    sql.modify.column(table_name, modify='add', column_name='B', data_type='VARCHAR(20)')
    schema = helpers.get_schema(sql.connection, table_name)
    assert 'B' in schema.index
    assert schema.at['B','data_type']=='varchar'
    assert schema.at['B','max_length']==20

    sql.modify.column(table_name, modify='add', column_name='C', data_type='BIGINT')
    schema = helpers.get_schema(sql.connection, table_name)
    assert 'C' in schema.index
    assert schema.at['C','data_type']=='bigint' 


def test_column_alter(sql):

    table_name = '##test_column_alter'
    columns = {"A": "VARCHAR(10)", "B": "BIGINT", "C": "BIGINT", "D": "BIGINT"}
    sql.create.table(table_name, columns)

    sql.modify.column(table_name, modify='alter', column_name='B', data_type='INT')
    schema = helpers.get_schema(sql.connection, table_name)
    assert 'B' in schema.index
    assert schema.at['B','data_type']=='int'
    assert schema.at['B', 'is_nullable']==True

    sql.modify.column(table_name, modify='alter', column_name='C', data_type='INT', not_null=True)
    schema = helpers.get_schema(sql.connection, table_name)
    assert 'C' in schema.index
    assert schema.at['C','data_type']=='int'
    assert schema.at['C', 'is_nullable']==False


def test_primary_key_input_error(sql):

    table_name = '##test_primary_key_input_error'
    columns = {"A": "INT", "B": "BIGINT", "C": "BIGINT", "D": "BIGINT"}
    sql.create.table(table_name, columns, not_null=["A","B"])

    with pytest.raises(ValueError):
        sql.modify.primary_key(table_name, modify='create', columns=['A','B'], primary_key_name = '_pk_1') 


def test_primary_key_one_column(sql):

    table_name = '##test_primary_key_one_column'
    columns = {"A": "INT", "B": "BIGINT", "C": "BIGINT", "D": "BIGINT"}
    sql.create.table(table_name, columns, not_null=["A","B"])

    sql.modify.primary_key(table_name, modify='add', columns='A', primary_key_name = '_pk_1')
    schema = helpers.get_schema(sql.connection, table_name)
    assert schema.at['A','is_primary_key']==True
    assert sum(schema['is_primary_key'])==1

    sql.modify.primary_key(table_name, modify='drop', columns='A',  primary_key_name = '_pk_1')
    schema = helpers.get_schema(sql.connection, table_name)
    assert schema.at['A','is_primary_key']==False
    assert sum(schema['is_primary_key'])==0


def test_primary_key_two_columns(sql):

    table_name = '##test_primary_key_two_columns'
    columns = {"A": "INT", "B": "BIGINT", "C": "BIGINT", "D": "BIGINT"}
    sql.create.table(table_name, columns, not_null=["A","B"])

    sql.modify.primary_key(table_name, modify='add', columns=['A','B'], primary_key_name = '_pk_1')
    schema = helpers.get_schema(sql.connection, table_name)
    assert schema.at['A','is_primary_key']==True
    assert schema.at['B','is_primary_key']==True
    assert sum(schema['is_primary_key'])==2

    sql.modify.primary_key(table_name, modify='drop', columns=['A','B'],  primary_key_name = '_pk_1')
    schema = helpers.get_schema(sql.connection, table_name)
    assert schema.at['A','is_primary_key']==False
    assert schema.at['B','is_primary_key']==False
    assert sum(schema['is_primary_key'])==0


def test_alter_primary_key_column(sql):

    table_name = "##test_alter_primary_key_column"
    columns = {"_pk": "TINYINT", "A": 'VARCHAR(1)'}
    sql.create.table(table_name, columns, primary_key_column = "_pk")

    primary_key_name, primary_key_column = helpers.get_pk_details(sql.connection, table_name)

    sql.modify.primary_key(table_name, modify='drop', columns=primary_key_column, primary_key_name=primary_key_name)
    sql.modify.column(table_name, modify='alter', column_name=primary_key_column, data_type='INT', not_null=True)
    sql.modify.primary_key(table_name, modify='add', columns=primary_key_column, primary_key_name=primary_key_name)

    schema = helpers.get_schema(sql.connection, table_name)
    assert schema.at['_pk','data_type']=='int'
    assert schema.at['_pk','is_primary_key']==True