import warnings

import pandas as pd

from mssql_dataframe.core import errors, conversion, dynamic, infer, modify, create


class insert():

    def __init__(self, connection, adjust_sql_objects: bool = False, adjust_sql_attempts: int = 10, fast_executemany: bool = True):

        self.connection = connection
        self.adjust_sql_objects = adjust_sql_objects
        self.adjust_sql_attempts = adjust_sql_attempts
        self.fast_executemany = fast_executemany

        self.modify = modify.modify(self.connection)
        self.create = create.create(self.connection)


    def insert(self,table_name: str, dataframe: pd.DataFrame, include_timestamps: bool = True):
        """Insert data into SQL table from a dataframe.

        Parameters
        ----------

        table_name (str) : name of table to insert data into
        dataframe (pandas.DataFrame): tabular data to insert
        include_timestamps (bool, default=True) : include _time_insert column in server time

        Returns
        -------
        
        dataframe (pandas.DataFrame) : inserted data that may have been altered to conform to SQL
        schema (pandas.DataFrame) : properties of SQL table columns where data was inserted

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
        if include_timestamps:
            additional_columns = ['_time_insert']
        else:
            additional_columns = None
        for attempt in range(0,self.adjust_sql_attempts):
            try:
                schema, dataframe = conversion.get_schema(self.connection.connection, table_name, dataframe, additional_columns)
                break
            except (errors.SQLTableDoesNotExist, errors.SQLColumnDoesNotExist, errors.SQLInsufficientColumnSize) as failure:
                self.handle(failure, table_name, dataframe)
            if attempt==self.adjust_sql_attempts-1:
                raise RecursionError(f'adjust_sql_attempts={self.adjust_sql_attempts} reached')

        # dynamic SQL object names
        table = dynamic.escape(cursor, table_name)
        columns = dynamic.escape(cursor, dataframe.columns)

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

        return dataframe, schema


    def handle(self, failure, table_name: str, dataframe: pd.DataFrame):
        '''Handle a failed write attempt.
        
        Parameters
        ----------
        failure (mssql_dataframe.core.errors) : exception to potentially handle
        table_name (str) : name of the table for which the failed write attempt occured
        dataframe (pandas.DataFrame) : data to insert

        Returns
        -------
        dataframe (pandas.DataFrame) : data to insert
        
        '''
        # check if specific columns initiated the failure
        columns = pd.Series([], dtype='string')
        if len(failure.args)>1:
            columns = pd.Series(failure.args[1], dtype='string')

        # always add include_timestamps columns, regardless of adjust_sql_objects value
        include_timestamps = ['_time_insert','_time_update']
        if isinstance(failure, errors.SQLColumnDoesNotExist) and all(columns.isin(include_timestamps)):
            for col in columns:
                warnings.warn('Creating column {} in table {} with data type DATETIME2.'.format(col, table_name), errors.SQLObjectAdjustment)
                self.modify.column(table_name, modify='add', column_name=col, data_type='DATETIME2')

        elif self.adjust_sql_objects==False:
            raise failure

        elif isinstance(failure, errors.SQLTableDoesNotExist):
            warnings.warn('Creating table {}'.format(table_name), errors.SQLObjectAdjustment)
            dataframe = self.create.table_from_dataframe(table_name, dataframe, primary_key='infer')

        elif isinstance(failure, errors.SQLColumnDoesNotExist):
            # infer the data types for new columns
            new, dtypes, _, _ = infer.sql(dataframe[columns])
            # add size to string columns
            dtypes = conversion.string_size(dtypes, new)
            strings = dtypes[dtypes['sql_type'].isin(['varchar','nvarchar'])].index
            dtypes['odbc_size'] = dtypes['odbc_size'].astype('string')
            dtypes.loc[strings,'sql_type'] = dtypes.loc[strings,'sql_type']+'('+dtypes.loc[strings,'odbc_size']+')'
            dtypes = dtypes['sql_type'].to_dict()
            # add each column
            for col, spec in dtypes.items():
                self.modify.column(table_name, modify='add', column_name=col, data_type=spec, notnull=False)
            # add potentially adjusted columns back into dataframe
            dataframe[new.columns] = new

        elif isinstance(failure, errors.SQLInsufficientColumnSize):
            pass

        return dataframe