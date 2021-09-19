import warnings

import pandas as pd
pd.options.mode.chained_assignment = 'raise'

from mssql_dataframe.core import errors, conversion, dynamic, infer, modify, create


class insert():

    def __init__(self, connection, adjust_sql_objects: bool = False, adjust_sql_attempts: int = 3):

        self.connection = connection
        self.adjust_sql_objects = adjust_sql_objects
        self.adjust_sql_attempts = adjust_sql_attempts

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
        cursor.fast_executemany = True

        # get target table schema, while checking for errors and adjusting data for inserting
        if include_timestamps:
            additional_columns = ['_time_insert']
        else:
            additional_columns = None
        schema, dataframe = self.target(table_name, dataframe, cursor, additional_columns)

        # column names from dataframe contents
        if any(dataframe.index.names):
            # named index columns will also have values returned from conversion.prepare_values
            columns = list(dataframe.index.names)+list(dataframe.columns)
        else:
            columns = dataframe.columns

        # dynamic SQL object names
        table = dynamic.escape(cursor, table_name)
        columns = dynamic.escape(cursor, columns)

        # prepare values of dataframe for insert
        dataframe, values = conversion.prepare_values(schema, dataframe)

        # prepare cursor for input data types and sizes
        cursor = conversion.prepare_cursor(schema, dataframe, cursor)

        # issue insert statement
        if include_timestamps:
            insert = "_time_insert, "+', '.join(columns)
            params = "GETDATE(), "+", ".join(["?"]*len(columns))
        else:
            insert = ', '.join(columns)
            params = ", ".join(["?"]*len(columns))
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


    def handle(self, failure, table_name: str, dataframe: pd.DataFrame, updating_table: bool):
        '''Handle a failed write attempt.
        
        Parameters
        ----------
        failure (mssql_dataframe.core.errors) : exception to potentially handle
        table_name (str) : name of the table for which the failed write attempt occured
        dataframe (pandas.DataFrame) : data to insert
        updating_table (bool, default=False) : flag that indicates if target table is being updated

        Returns
        -------
        dataframe (pandas.DataFrame) : data to insert
        
        '''
        # check if specific columns initiated the failure
        if len(failure.args)>1:
            columns = pd.Series(failure.args[1], dtype='string')
        else:
            columns = pd.Series([], dtype='string')

        # always add include_timestamps columns, regardless of adjust_sql_objects value
        include_timestamps = ['_time_insert','_time_update']
        if isinstance(failure, errors.SQLColumnDoesNotExist) and all(columns.isin(include_timestamps)):
            for col in columns:
                warnings.warn(f'Creating column {col} in table {table_name} with data type DATETIME2.', errors.SQLObjectAdjustment)
                self.modify.column(table_name, modify='add', column_name=col, data_type='DATETIME2')

        elif self.adjust_sql_objects==False:
            raise failure

        elif isinstance(failure, errors.SQLTableDoesNotExist):
            if updating_table:
                raise failure
            else:
                dataframe = self.create_table(table_name, dataframe)

        elif isinstance(failure, errors.SQLColumnDoesNotExist):
            dataframe = self.add_columns(table_name, dataframe, columns)

        elif isinstance(failure, errors.SQLInsufficientColumnSize):
            dataframe = self.alter_columns(table_name, dataframe, columns)

        return dataframe

    
    def create_table(self, table_name, dataframe):

        warnings.warn('Creating table {}'.format(table_name), errors.SQLObjectAdjustment)
        dataframe = self.create.table_from_dataframe(table_name, dataframe, primary_key='infer')

        return dataframe


    def add_columns(self, table_name, dataframe, columns):

        # infer the data types for new columns
        new, schema, _, _ = infer.sql(dataframe.loc[:,columns])
        # determine the SQL data type for each column
        _, dtypes = conversion.sql_spec(schema, new)
        # add each column
        for col, spec in dtypes.items():
            warnings.warn(f'Creating column {col} in table {table_name} with data type {spec}.', errors.SQLObjectAdjustment)
            self.modify.column(table_name, modify='add', column_name=col, data_type=spec, is_nullable=True)
        # add potentially adjusted columns back into dataframe
        dataframe[new.columns] = new

        return dataframe


    def alter_columns(self, table_name, dataframe, columns):

        # temporarily set named index (primary key) as columns
        index = dataframe.index.names
        if any(index):
            dataframe = dataframe.reset_index()
        # infer the data types for insufficient size columns
        new, schema, _, _ = infer.sql(dataframe.loc[:,columns])
        schema, dtypes = conversion.sql_spec(schema, new)
        # get current table schema
        previous, _ = conversion.get_schema(self.connection.connection, table_name)
        strings = previous['sql_type'].isin(['varchar','nvarchar'])
        previous.loc[strings,'odbc_size'] = previous.loc[strings,'column_size']
        # insure change within the same sql data type category after inferring dtypes
        unchanged = previous.loc[schema.index,['sql_type','odbc_size']]==schema[['sql_type','odbc_size']]
        unchanged = unchanged.all(axis='columns')
        if any(unchanged):
            unchanged = list(unchanged[unchanged].index)
            raise errors.SQLRecastColumnUnchanged(f'Handling SQLInsufficientColumnSize did not result in type or size change for columns: {unchanged}')
        # insure change doesn't result in different sql data category
        changed = previous.loc[schema.index,['sql_category']]!=schema[['sql_category']]
        if any(changed['sql_category']):
            changed = list(changed[changed['sql_category']].index)
            raise errors.SQLRecastColumnChangedCategory(f'Handling SQLInsufficientColumnSize resulted in data type category change for columns: {changed}')
        # drop primary key constraint prior to altering columns, if needed
        primary_key_columns = previous.loc[previous['pk_seq'].notna(), 'pk_seq'].sort_values(ascending=True).index
        if len(primary_key_columns)==0:
            primary_key_name = None
        else:
            primary_key_name = previous.loc[primary_key_columns[0],'pk_name']
            self.modify.primary_key(table_name, modify='drop', columns=primary_key_columns, primary_key_name=primary_key_name)
        # alter each column
        for col, spec in dtypes.items():
            is_nullable = previous.at[col,'is_nullable']
            warnings.warn(f'Altering column {col} in table {table_name} to data type {spec} with is_nullable={is_nullable}.', errors.SQLObjectAdjustment)
            self.modify.column(table_name, modify='alter', column_name=col, data_type=spec, is_nullable=is_nullable)
        # readd primary key if needed
        if primary_key_name:
            self.modify.primary_key(table_name, modify='add', columns=list(primary_key_columns), primary_key_name=primary_key_name)
        # reset primary key columns as dataframe's index
        if any(index):
            dataframe = dataframe.set_index(keys=index)

        return dataframe


    def target(self, table_name, dataframe, cursor, additional_columns: list = None, updating_table: bool=False):
        ''' Get target schema, potentially handle errors, and adjust dataframe contents before inserting into target table.

        Parameters
        ----------
        table_name (str) : name of target table
        dataframe (pandas.DataFrame): tabular data to insert
        cursor (pyodbc.connection.cursor) : cursor to execute statement
        additional_columns (list, default=None) : columns that will be generated by an SQL statement but not in the dataframe       
        updating_table (bool, default=False) : flag that indicates if target table is being updated

        Returns
        -------
        schema (pandas.DataFrame) : table column specifications and conversion rules
        dataframe (pandas.DataFrame) : input dataframe with optimal values and types for inserting into SQL
        '''

        for attempt in range(0, self.adjust_sql_attempts+1):
            try:
                schema, dataframe = conversion.get_schema(self.connection.connection, table_name, dataframe, additional_columns)
                break
            except (errors.SQLTableDoesNotExist, errors.SQLColumnDoesNotExist, errors.SQLInsufficientColumnSize) as failure:
                cursor.rollback()
                if attempt==self.adjust_sql_attempts:
                    raise RecursionError(f'adjust_sql_attempts={self.adjust_sql_attempts} reached')
                self.handle(failure, table_name, dataframe, updating_table)
                cursor.commit()
            except Exception as err:
                cursor.rollback()
                raise err

        return schema, dataframe


    def source(self, table_name, dataframe, cursor, match_columns: list = None, additional_columns: list = None,
        updating_table: bool = False):
        '''Create a source table with data in SQL for update and merge operations.
        
        Parameters
        ----------
        table_name (str) : name of target table
        dataframe (pandas.DataFrame): tabular data to insert
        cursor (pyodbc.connection.cursor) : cursor to execute statement
        match_columns (list|str) : columns to match records to updating/merging, if None the primary key is used
        additional_columns (list, default=None) : columns that will be generated by an SQL statement but not in the dataframe  
        updating_table (bool, default=False) : flag that indicates if target table is being updated

        Returns
        -------

        
        '''
        if isinstance(match_columns,str):
            match_columns = [match_columns]

        # get target table schema, while checking for errors and adjusting data for inserting
        schema, dataframe = self.target(table_name, dataframe, cursor, additional_columns, updating_table)

        # use primary key if match_columns is not given
        if match_columns is None:
            match_columns = list(schema[schema['pk_seq'].notna()].index)
            if not match_columns:
                raise errors.SQLUndefinedPrimaryKey('SQL table {} has no primary key. Either set the primary key or specify the match_columns'.format(table_name))
        # match_column presence is SQL table
        missing = [x for x in match_columns if x not in schema.index]
        if missing:
            raise errors.SQLColumnDoesNotExist(f'match_columns not found in SQL table {table_name}', missing)
        # match_column presence in dataframe
        missing = [x for x in match_columns if x not in list(dataframe.index.names)+list(dataframe.columns)]
        if missing:
            raise errors.DataframeUndefinedColumn('match_columns not found in dataframe', missing)

        # insert data into source temporary table
        temp_name = '##__source_'+table_name
        columns = list(dataframe.columns)
        if any(dataframe.index.names):
            columns = list(dataframe.index.names)+columns
        _, dtypes = conversion.sql_spec(schema.loc[columns], dataframe)
        dtypes = {k:v.replace('int identity','int') for k,v in dtypes.items()}
        not_nullable = list(schema[schema['is_nullable']==False].index)
        self.create.table(temp_name, dtypes, not_nullable, primary_key_column=match_columns)
        _, _ = self.insert(temp_name, dataframe, include_timestamps=False)

        return schema, dataframe, match_columns, temp_name