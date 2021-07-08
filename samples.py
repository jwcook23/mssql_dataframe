import pandas as pd

from mssql_dataframe.connect import connect
from mssql_dataframe.collection import SQLServer

db = connect(database_name='master', server_name='localhost')
sql = SQLServer(db)

table_name = '##sample_update'

# create table to update
dataframe = pd.DataFrame({
    'ColumnA': [1,2],
    'ColumnB': ['a','b'],
    'ColumnC': [3,4]
})
dataframe = sql.create.table_from_dataframe(table_name, dataframe, primary_key='index')
sql.write.insert(table_name, dataframe)

# update values in table, using the primary key
dataframe['ColumnC'] = [5,6]
sql.write.update(table_name, dataframe[['ColumnC']])