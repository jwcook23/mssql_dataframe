import pandas as pd

from mssql_dataframe.connect import connect
from mssql_dataframe.collection import SQLServer
from mssql_dataframe.core.helpers import get_schema

db = connect(database_name='master', server_name='localhost')
# # initialize the main package
sql = SQLServer(db, adjust_sql_objects=True)

dataframe = pd.read_csv(r'C:\Users\jacoo\Desktop\Temp\test.csv')
dataframe = dataframe[dataframe.columns[1::]]

table_name = '##test_error'

# Expire-Date column causes invalid character value for cast specification
# # can't insert date values with format 06/22/2021
# # TODO: add a test for this case in write
# # TODO: ? first convert the python type
# columns = dataframe.columns[0:4]
columns = dataframe.columns[3]
sql.write.merge(table_name, dataframe.loc[0:10, [columns]])
# dataframe = sql.create.table_from_dataframe(table_name, dataframe.loc[0:10, [columns]])

schema = get_schema(db, table_name)