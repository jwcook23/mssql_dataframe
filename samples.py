import time
import pandas as pd

# Initialize mssql_dataframe
from mssql_dataframe.connect import connect
from mssql_dataframe.collection import SQLServer

# connect to an on-premise database using pyodbc and Windows authentication
db = connect(database_name='master', server_name='localhost')
# # or an Azure SQL database
# db = connect(server_name='<server>.database.windows.net', username='<username>', password='<password>')

# initialize with the ability to create and alter objects if needed
sql = SQLServer(db, adjust_sql_objects=True)
# # or without the ability to modify SQL objects
# sql = SQLServer(db)
 

# Updating an SQL Table
# # 1. create global SQL temp table using a dataframe (for demonstration, in practice would be a regular table)
# # 2. insert initial records from dataframe into SQL
# # 3. update the records in the dataframe
# # 4. update the records in SQL
table_name = '##sample_update'
dataframe = pd.DataFrame({
    'ColumnA': list(range(0,100000,1)),
    'ColumnB': [0]*100000 
})

# # create table in SQL using the dataframe
# # a. index of the dataframe is set as the SQL primary key
# # b. "best fit" SQL data type is derived in SQL
dataframe = sql.create.table_from_dataframe(table_name, dataframe, primary_key='index')

# # insert initial records into empty table
# # a. records are inserted, taking care of any missing type values in the dataframe
time_start = time.time()
sql.write.insert(table_name, dataframe)
print('Inserted {} records in {} seconds'.format(len(dataframe), round(time.time()-time_start),2))
# # > > Inserted 100000 records in 1.8 seconds

# update values in SQL table using dataframe's, which was set as the SQL primary key
dataframe['ColumnB'] = list(range(0,100000,1))
dataframe['ColumnC'] = 1
time_start = time.time()
sql.write.update(table_name, dataframe[['ColumnB','ColumnC']])
print('Updated {} records in {} seconds'.format(len(dataframe), round(time.time()-time_start),2))