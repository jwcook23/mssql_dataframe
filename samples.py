# Initialization
from mssql_dataframe.connect import connect
from mssql_dataframe.collection import SQLServer

# connect to an on-premise database using pyodbc and Windows authentication
db = connect(database_name='master', server_name='localhost')
# # or an Azure SQL database
# db = connect(server_name='<server>.database.windows.net', username='<username>', password='<password>')

# using a single connection, initialize the class
sql = SQLServer(db)
# # or initialize the class with the ability to modify SQL objects as needed
#sql = SQLServer(db, adjust_sql_objects=True)
 

import pandas as pd
import time

from mssql_dataframe.connect import connect
from mssql_dataframe.collection import SQLServer

db = connect(database_name='master', server_name='localhost')
sql = SQLServer(db, adjust_sql_objects=True)

table_name = '##sample_update'

# create table to update
dataframe = pd.DataFrame({
    'UnaffectedColumn': list(range(0,100000,1)),
    'UpdateColumn': [0]*100000 
})
dataframe = sql.create.table_from_dataframe(table_name, dataframe, primary_key='index', row_count=1)
sql.write.insert(table_name, dataframe)

# update values in table, using the SQL primary key
dataframe['UpdateColumn'] = list(range(0,100000,1))
sql.write.update(table_name, dataframe[['UpdateColumn']])