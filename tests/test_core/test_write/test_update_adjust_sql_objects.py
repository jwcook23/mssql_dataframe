import warnings

import pytest
import pandas as pd
pd.options.mode.chained_assignment = 'raise'

from mssql_dataframe import connect
from mssql_dataframe.core import errors, create, conversion
from mssql_dataframe.core.write import insert

class package:
    def __init__(self, connection):
        self.connection = connection
        self.create = create.create(connection)
        self.insert = insert.insert(connection, adjust_sql_objects=True)

@pytest.fixture(scope="module")
def sql():
    db = connect.connect(database_name='tempdb', server_name='localhost')
    yield package(db)
    db.connection.close()


# def test_update_create_table(sql_adjustable):

#     table_name = '##test_update_create_table'

#     # create table to update
#     dataframe = pd.DataFrame({
#         '_pk': [0,1],
#         'ColumnA': [1,2]
#     }).set_index(keys='_pk')
    
#     with pytest.raises(errors.SQLTableDoesNotExist):
#         sql_adjustable.write.update(table_name, dataframe)


# def test_update_add_column(sql_adjustable):
    
#     table_name = '##test_update_add_column'
#     dataframe = pd.DataFrame({
#         'ColumnA': [1,2]
#     })
#     with warnings.catch_warnings(record=True) as warn:
#         sql_adjustable.create.table_from_dataframe(table_name, dataframe, primary_key='index')
#         assert len(warn)==1
#         assert isinstance(warn[0].message, errors.SQLObjectAdjustment)
#         assert 'Created table' in str(warn[0].message)
#     sql_adjustable.write.insert(table_name, dataframe, include_timestamps=False)

#     # update using the SQL primary key that came from the dataframe's index
#     dataframe['NewColumn'] = [3,4]
#     with warnings.catch_warnings(record=True) as warn:
#         sql_adjustable.write.update(table_name, dataframe[['NewColumn']])
#         assert len(warn)==2
#         assert all([isinstance(x.message, errors.SQLObjectAdjustment) for x in warn])
#         assert 'Creating column NewColumn in table '+table_name in str(warn[0].message)
#         assert 'Creating column _time_update in table '+table_name in str(warn[1].message)
#         result = sql_adjustable.read.select(table_name)
#         assert all(result[['ColumnA','NewColumn']]==dataframe)
#         assert (result['_time_update'].notna()).all()


# def test_update_alter_column(sql_adjustable):

#     table_name = '##test_update_alter_column'
#     dataframe = pd.DataFrame({
#         'ColumnA': [1,2],
#         'ColumnB': ['a','b'],
#         'ColumnC': [0,0]
#     })
#     with warnings.catch_warnings(record=True) as warn:
#         sql_adjustable.create.table_from_dataframe(table_name, dataframe, primary_key=None)
#         assert len(warn)==1
#         assert isinstance(warn[0].message, errors.SQLObjectAdjustment)
#         assert 'Created table' in str(warn[0].message)
#     sql_adjustable.write.insert(table_name, dataframe, include_timestamps=False)

#     # update using ColumnA
#     dataframe['ColumnB'] = ['aaa','bbb']
#     dataframe['ColumnC'] = [256, 256]
#     with warnings.catch_warnings(record=True) as warn:
#         sql_adjustable.write.update(table_name, dataframe, match_columns=['ColumnA'])
#         assert len(warn)==3
#         assert all([isinstance(x.message, errors.SQLObjectAdjustment) for x in warn])
#         assert 'Creating column _time_update in table '+table_name in str(warn[0].message)
#         assert 'Altering column ColumnB in table '+table_name in str(warn[1].message)
#         assert 'Altering column ColumnC in table '+table_name in str(warn[2].message)
#         results = sql_adjustable.read.select(table_name)
#         assert all(results[['ColumnA','ColumnB','ColumnC']]==dataframe[['ColumnA','ColumnB','ColumnC']])
#         assert all(results['_time_update'].notna())

#     schema = helpers.get_schema(sql_adjustable.connection, table_name)
#     columns,_,_,_ = helpers.flatten_schema(schema)
#     assert columns=={'ColumnA': 'tinyint', 'ColumnB': 'varchar(3)', 'ColumnC': 'smallint', '_time_update': 'datetime'}


# def test_update_add_and_alter_column(sql_adjustable):

#     table_name = '##test_update_add_and_alter_column'
#     dataframe = pd.DataFrame({
#         'ColumnA': [1,2],
#         'ColumnB': ['a','b']
#     })
#     with warnings.catch_warnings(record=True) as warn:
#         sql_adjustable.create.table_from_dataframe(table_name, dataframe, primary_key='index')
#         assert len(warn)==1
#         assert isinstance(warn[0].message, errors.SQLObjectAdjustment)
#         assert 'Created table' in str(warn[0].message)
#     sql_adjustable.write.insert(table_name, dataframe, include_timestamps=False)

#     # update using the SQL primary key that came from the dataframe's index
#     dataframe['ColumnB'] = ['aaa','bbb']
#     dataframe['NewColumn'] = [3,4]
#     with warnings.catch_warnings(record=True) as warn:
#         sql_adjustable.write.update(table_name, dataframe[['ColumnB', 'NewColumn']])
#         assert len(warn)==3
#         assert all([isinstance(x.message, errors.SQLObjectAdjustment) for x in warn])
#         assert 'Creating column NewColumn in table '+table_name in str(warn[0].message)
#         assert 'Creating column _time_update in table '+table_name in str(warn[1].message)
#         assert 'Altering column ColumnB in table '+table_name in str(warn[2].message)
#         result = sql_adjustable.read.select(table_name)
#         assert all(result[['ColumnA','ColumnB','NewColumn']]==dataframe)
#         assert (result['_time_update'].notna()).all()