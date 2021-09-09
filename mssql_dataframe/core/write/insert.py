import pandas as pd

from mssql_dataframe.core import conversion


class insert():

    def __init__(self, connection, fast_executemany: bool = True):

        self.connection = connection
        self.fast_executemany = fast_executemany


    def insert(self,table_name: str, dataframe: pd.DataFrame, include_timestamps: bool = True):
        """Insert data into SQL table from a dataframe.

        Parameters
        ----------

        table_name (str) : name of table to insert data into
        dataframe (pandas.DataFrame): tabular data to insert
        include_timestamps (bool, default=True) : include _time_insert column in server time

        Returns
        -------
        
        None

        Examples
        --------

        #### include _time_insert by default
        write.insert('SomeTable', pd.DataFrame({'ColumnA': [1, 2, 3]}))

        #### do not include an insert time
        write.insert('SomeTable', pd.DataFrame({'ColumnA': [1, 2, 3]}), include_timestamps=False)

        """

        # create cursor to perform operations
        cursor = self.connection.connection.cursor()
        cursor.fast_executemany = self.fast_executemany

        # get table schema for setting input data types and sizes
        schema = conversion.get_schema(self.connection.connection, table_name, columns=dataframe.columns)

        # check dataframe contents against SQL schema to correctly raise or avoid exceptions
        dataframe = conversion.precheck_dataframe(schema, dataframe)

        # dynamic SQL object names
        table = conversion.escape(cursor, table_name)
        columns = conversion.escape(cursor, dataframe.columns)

        # prepare values of dataframe for insert
        dataframe, values = conversion.prepare_values(schema, dataframe)

        # prepare cursor for input data types and sizes
        cursor = conversion.prepare_cursor(schema, dataframe, cursor)

        # issue insert statement
        if include_timestamps:
            insert = "_time_insert, "+', '.join(columns)
            params = "GETDATE(), "+", ".join(["?"]*len(dataframe.columns))
        else:
            insert = ', '.join(columns)
            params = ", ".join(["?"]*len(dataframe.columns))
        statement = f"""
        INSERT INTO
        {table} (
            {insert}
        ) VALUES (
            {params}
        )
        """
        cursor.executemany(statement, values)
        cursor.commit()
