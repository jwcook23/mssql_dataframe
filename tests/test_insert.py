import pytest
import pandas as pd

from mssql_dataframe import connect
from mssql_dataframe import table
from mssql_dataframe import insert

@pytest.fixture(scope="module")
def connection():
    db = connect.SQLServer(database_name='tempdb', server_name='localhost', autocommit=False)
    sql = {'table': table.table(db), 'insert': insert.insert(db)}
    yield sql
    db.connection.close()


def test_insert_value(connection):

    table_name = '##InsertValue'
    columns = {'ColumnA': 'TINYINT'}
    dataframe = pd.DataFrame({'ColumnA': [1]})

    connection['table'].create_table(table_name, columns)
    connection['insert'].insert_data(table_name, dataframe)


# def test_insert_column(connection):

#     name = '##test_insert_column'
#     columns = {data["complex"].pk: data["complex"].columns[data["complex"].pk]}
#     dataframe = data["complex"].dataframe[[data["complex"].pk]]

#     connection['table'].create_table(name, columns)
    
#     connection['insert'].insert_data(name, dataframe)


# def test_insert_dataframe(connection):

#     name = '##test_insert_dataframe'
#     columns = data["complex"].columns
#     dataframe = data["complex"].dataframe

#     connection['table'].create_table(name, columns)
    
#     connection['insert'].insert_data(name, dataframe)