import pandas as pd
import numpy as np


class insert():
    """ Insert data into existing SQL tables.

    Parameters
    ----------

    connection (mssql_dataframe.create_sql.connection, default = None) : execute if not None

    Returns
    -------

    None

    """

    def __init__(self, connection = None):
        
        self.connection = connection



    def insert_data(self, table_name: str, dataframe: pd.DataFrame):
        """Develop SQL statement for inserting data.

        Parameters
        ----------

        name (str) : name of table to create
        dataframe (pd.DataFrame): tabular data to insert

        Returns
        -------
        
        statement (str) : query statement string to pass to execute method
        values (list) : values to pass to execute method

        Examples
        --------

        data = pd.DataFrame({'ColumnA': [1, 2, 3]})
        statement, values = execute_statement.insert_data('TableName', data)
        cursor.executemany(statement, values)

        """    

        # interpret any kind of missing values as NULL in SQL
        dataframe = dataframe.fillna(np.nan).replace([np.nan], [None])

        # extract values to insert into a list of lists
        values = dataframe.values.tolist()

        # form parameterized statement
        columns = ", ".join(dataframe.columns)

        params = '('+', '.join(['?']*len(dataframe.columns))+')'
        
        statement = "INSERT INTO "+table_name+" ("+columns+") VALUES "+params

        # insert values
        self.connection.cursor.executemany(statement, values)


# class merge():

#     def __init__(self):

#         self.statement = """
#             MERGE {table} AS _target
#             USING #{table}_merge AS _source 
#             ON {_pk}
#             WHEN MATCHED THEN
#                 UPDATE SET {_update}
#             WHEN NOT MATCHED BY TARGET THEN
#                 INSERT ({_insert}) VALUES ({_values});
#         """

#             # WHEN NOT MATCHED BY SOURCE THEN
#             #     DELETE  


#     def merge(self,dataset):
#         """
#         Merge dataframe into SQL using a temporary table and a T-SQL MERGE statement.

#         Parameters

#             dataset         dataframe               data to merge into SQL table
#             update          bool, default=True      (WHEN MATCHED)
#             insert          bool, default=True      (WHEN NOT MATCHED BY TARGET)
#             delete          bool, default=False     (WHEN NOT MATCHED BY SOURCE)

#         Returns

#             None
        
#         """

#         statement = self.statement.format(
#             _table = table.name,
#             _temp = update.name, 
#             _pk = ', '.join(['_target.'+x+'=_source.'+x for x in pk]),
#             _update = ', '.join(['_target.'+x+'=_source.'+x for x in non_pks]),
#             # auto increment
#             # _insert = ', '.join(non_pks),
#             # _values = ', '.join(['_source.'+x for x in non_pks])
#             # non-auto increment
#             _insert = ', '.join(pk+non_pks),
#             _values = ', '.join(['_source.'+x for x in pk+non_pks])        
#         )


#     def temp_table(self):
#         """
        
#         """
#         pass