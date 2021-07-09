import pandas as pd
import time

from mssql_dataframe.connect import connect
from mssql_dataframe.collection import SQLServer

db = connect(database_name='master', server_name='localhost')
sql = SQLServer(db)

table_name = '##sample_update'

# create table to update
dataframe = pd.DataFrame({
    'UnaffectedColumn': list(range(0,100000,1)),
    'UpdateColumn': [0]*100000 
})
dataframe = sql.create.table_from_dataframe(table_name, dataframe, primary_key='index', row_count=1)
sql.write.insert(table_name, dataframe)

# update values in table, using the primary key
dataframe['UpdateColumn'] = list(range(0,100000,1))
sql.write.update(table_name, dataframe[['UpdateColumn']])