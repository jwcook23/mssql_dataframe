import pandas as pd
import numpy as np


class insert():
    """ Creates SQL statements for interacting with table objects.

    Parameters
    ----------

    connection (mssql_dataframe.create_sql.connection, default = None) : execute if not None

    Returns
    -------

    None

    """

    def __init__(self, connection = None):
        
        self.connection = connection



    def insert_data(self, name: str, dataframe: pd.DataFrame):
        """Develop SQL statement for inserting data.

        Parameters
        ----------

        name (str) : name of table to create

        dataframe(pd.DataFrame): tabular data to insert

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
        
        statement = "INSERT INTO "+name+" ("+columns+") VALUES "+params

        # insert values
        self.connection.cursor.executemany(statement, values)