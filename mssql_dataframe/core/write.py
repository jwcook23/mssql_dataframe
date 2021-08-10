from typing import Literal
import warnings
import re
from numpy.lib.arraysetops import isin

import pandas as pd
import numpy as np
import pyodbc

from mssql_dataframe.core import errors, helpers, create, modify


class write():

    def __init__(self, connection, adjust_sql_objects: bool = False, adjust_sql_attempts: int = 10):
        '''Class for writing to SQL tables.
        
        Parameters
        ----------
        connection (mssql_dataframe.connect) : connection for executing statement
        adjust_sql_objects (bool) : create and modify SQL tables and columns as needed if True
        adjust_sql_attempts (int, default=10) : maximum attempts at adjusting_sql_objects after write failure
        '''

        self.__connection__ = connection
        self.__create__ = create.create(connection)
        self.__modify__ = modify.modify(connection)
        self.adjust_sql_objects = adjust_sql_objects
        self.adjust_sql_attempts = adjust_sql_attempts


    def insert(self, table_name: str, dataframe: pd.DataFrame,
    include_timestamps: bool = True):
        """Insert data into SQL table from a dataframe.

        Parameters
        ----------

        table_name (str) : name of table to insert data into
        dataframe (pandas.DataFrame): tabular data to insert
        include_timestamps (bool, default=True) : include _time_insert column which is in server time

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

        # write index column as it is the primary key
        if any(dataframe.index.names):
            dataframe = dataframe.reset_index()

        # sanitize table and column names for safe sql
        table_clean = helpers.safe_sql(self.__connection__, table_name)
        column_names = ",\n".join(helpers.safe_sql(self.__connection__, dataframe.columns))

        # optionally add server insert time
        if include_timestamps:
            column_names = "_time_insert,\n"+column_names
            parameters = "GETDATE(), "+", ".join(["?"]*len(dataframe.columns))
        else:
            parameters = ", ".join(["?"]*len(dataframe.columns))

        # insert values
        statement = """
        INSERT INTO
        {table_name} (
            {column_names}
        ) VALUES (
            {parameters}
        )
        """
        statement = statement.format(
            table_name=table_clean, 
            column_names=column_names,
            parameters=parameters
        )

        # execute statement, and potentially handle errors
        self.__attempt_write(table_name, dataframe, self.__connection__.cursor.executemany, statement)


    def update(self, table_name: str, dataframe: pd.DataFrame, match_columns: list = None,
    include_timestamps: bool = True):
        """Update column(s) in an SQL table using a dataframe.

        Parameters
        ----------

        table_name (str) : name of table to insert data into
        dataframe (pandas.DataFrame): tabular data to insert
        match_columns (list, default=None) : matches records between dataframe and SQL table, if None the SQL primary key is used
        include_timestamps (bool, default=True) : include _time_update column which is in server time

        Returns
        -------
        
        None

        Examples
        --------

        #### update ColumnA only using the dataframe index & SQL primary key
        write.update('SomeTable', dataframe[['ColumnA']])

        #### update ColumnA and do not include a _time_update column value
        write.update('SomeTable', dataframe[['ColumnA']], include_timestamps=False)
        
        #### update Column A based on ColumnB and ColumnC, that do not have to be the SQL primary key
        write.update('SomeTable', dataframe[['ColumnA','ColumnB','ColumnC']], match_columns=['ColumnB','ColumnC'])

        """

        # perform pre-update steps
        try:
            dataframe, match_columns, table_temp = self.__prep_update_merge(table_name, match_columns, dataframe, operation='update')
        except errors.SQLTableDoesNotExist:
            msg = 'Attempt to update {} which does not exist.'.format(table_name)
            if self.adjust_sql_objects:
                msg += ' Parameter adjust_sql_objects=True does not apply when attempting to update a non-existant table.'
            raise errors.SQLTableDoesNotExist(msg)

        # develop basic update syntax
        statement = """
            DECLARE @SQLStatement AS NVARCHAR(MAX);
            DECLARE @TableName SYSNAME = ?;
            DECLARE @TableTemp SYSNAME = ?;
            {declare}

            SET @SQLStatement = 
                N'UPDATE '+
                    QUOTENAME(@TableName)+
                ' SET '+ 
                    {update_syntax}+
                ' FROM '+
                    QUOTENAME(@TableName)+' AS _target '+
                ' INNER JOIN '+
                    QUOTENAME(@TableTemp)+' AS _source '+
                    'ON '+{match_syntax}+';'
            EXEC sp_executesql 
                @SQLStatement,
                N'@TableName SYSNAME, @TableTemp SYSNAME, {parameters}',
                @TableName=@TableName, @TableTemp=@TableTemp, {values};
        """
        
        # update all columns in dataframe besides match columns
        update_columns = list(dataframe.columns[~dataframe.columns.isin(match_columns)])

        # alias columns to prevent direct input into SQL string
        alias_match = [str(x) for x in list(range(0,len(match_columns)))]
        alias_update = [str(x) for x in list(range(0,len(update_columns)))]

        # declare SQL variables
        declare = ["DECLARE @Match_"+x+" SYSNAME = ?;" for x in alias_match]
        declare += ["DECLARE @Update_"+x+" SYSNAME = ?;" for x in alias_update]
        declare = "\n".join(declare)

        # form inner join match syntax
        match_syntax = ["QUOTENAME(@Match_"+x+")" for x in alias_match]
        match_syntax = "+' AND '+".join(["'_target.'+"+x+"+'=_source.'+"+x for x in match_syntax])

        # form update syntax
        update_syntax = ["QUOTENAME(@Update_"+x+")" for x in alias_update]
        update_syntax = "+','+".join([x+"+'=_source.'+"+x for x in update_syntax])
        if include_timestamps:
            update_syntax = "'_time_update=GETDATE(),'+"+update_syntax

        # parameters for sp_executesql
        parameters = ["@Match_"+x+" SYSNAME" for x in alias_match]
        parameters += ["@Update_"+x+" SYSNAME" for x in alias_update]
        parameters =  ", ".join(parameters)

        # values for sp_executesql
        values = ["@Match_"+x+"=@Match_"+x for x in alias_match]
        values += ["@Update_"+x+"=@Update_"+x for x in alias_update]
        values =  ", ".join(values)

        # set final SQL string
        statement = statement.format(
            declare=declare,
            match_syntax=match_syntax,
            update_syntax=update_syntax,
            parameters=parameters,
            values=values
        )

        # perform update
        args = [table_name, table_temp]+match_columns+update_columns

        # execute statement, and potentially handle errors
        self.__attempt_write(table_name, dataframe, self.__connection__.cursor.execute, statement, args)
        table_temp = helpers.safe_sql(self.__connection__, table_temp)
        self.__connection__.cursor.execute('DROP TABLE '+table_temp)
        self.__connection__.cursor.commit()


    def merge(self, table_name: str, dataframe: pd.DataFrame, match_columns: list = None, 
    delete_unmatched: bool = True, delete_conditions: list = None, include_timestamps: bool = True):
        ''' Merge a dataframe into an SQL table by updating, inserting, and/or deleting rows using Transact-SQL MERGE.
        With delete_unmatched=False, this effectively becomes an UPSERT action.

        Parameters
        ----------

        table_name (str) : name of the SQL table
        dataframe (pandas.DataFrame): tabular data to merge into SQL table
        match_columns (list, default=None) : combination of columns or index to determine matches, if None the SQL primary key is used
        delete_unmatched (bool, default=True) : delete records if they do not match
        delete_conditions (list, default=None) : additional criteria that needs to match to prevent records from being deleted
        include_timestamps (bool, default=True) : include _time_insert and _time_update columns that are in server time

        Returns
        -------
        
        None

        Examples
        --------

        #### merge ColumnA and ColumnB values based on the SQL primary key / index of the dataframe

        write.merge('SomeTable', dataframe[['ColumnA','ColumnB']])

        #### for incrementally merging from a dataframe, require ColumnC also matches to prevent a record from being deleted

        write.merge('SomeTable', dataframe[['ColumnA','ColumnB', 'ColumnC']], delete_conditions=['ColumnC'])

        #### perform an UPSERT (if exists update, otherwise update) workflow

        write.merge('SomeTable', dataframe[['ColumnA']], delete_unmatched=False)

        '''

        if delete_conditions is not None and delete_unmatched==False:
            raise ValueError('delete_conditions can only be specified if delete_unmatched=True')

        # perform pre-merge steps
        try:
            dataframe, match_columns, table_temp = self.__prep_update_merge(table_name, match_columns, dataframe, operation='merge')
        # check for no table instead of relying on _attempt_write to prevent attempt to create a temp table
        except errors.SQLTableDoesNotExist:
            self.insert(table_name, dataframe, include_timestamps)
            return None

        # develop basic merge syntax
        statement = """
            DECLARE @SQLStatement AS NVARCHAR(MAX);
            DECLARE @TableName SYSNAME = ?;
            DECLARE @TableTemp SYSNAME = ?;
            {declare}

            SET @SQLStatement = 
            N' MERGE '+QUOTENAME(@TableName)+' AS _target '
            +' USING '+QUOTENAME(@TableTemp)+' AS _source '
            +' ON ('+{match_syntax}+') '
            +' WHEN MATCHED THEN UPDATE SET '+{update_syntax}
            +' WHEN NOT MATCHED THEN INSERT ('+{insert_syntax}+')'
            +' VALUES ('+{insert_values}+')'
            +{delete_syntax}+';'

            EXEC sp_executesql
                @SQLStatement,
                N'@TableName SYSNAME, @TableTemp SYSNAME, {parameters}',
                @TableName=@TableName, @TableTemp=@TableTemp, {values};
        """

        # if matched, update all columns in dataframe besides match_columns
        update_columns = list(dataframe.columns[~dataframe.columns.isin(match_columns)])

        # if not matched, insert all columns in dataframe
        insert_columns = list(dataframe.columns)

        # alias columns to prevent direct input into SQL string
        alias_match = [str(x) for x in list(range(0,len(match_columns)))]
        alias_update = [str(x) for x in list(range(0,len(update_columns)))]
        alias_insert = [str(x) for x in list(range(0,len(insert_columns)))]
        if delete_conditions is None:
            alias_conditions = []
        else:
            alias_conditions = [str(x) for x in list(range(0,len(delete_conditions)))]

        # declare SQL variables
        declare = ["DECLARE @Match_"+x+" SYSNAME = ?;" for x in alias_match]
        declare += ["DECLARE @Update_"+x+" SYSNAME = ?;" for x in alias_update]
        declare += ["DECLARE @Insert_"+x+" SYSNAME = ?;" for x in alias_insert]
        declare += ["DECLARE @Subset_"+x+" SYSNAME = ?;" for x in alias_conditions]
        declare = "\n".join(declare)

        # form match on syntax
        match_syntax = ["QUOTENAME(@Match_"+x+")" for x in alias_match]
        match_syntax = "+' AND '+".join(["'_target.'+"+x+"+'=_source.'+"+x for x in match_syntax])

        # form when matched then update syntax
        update_syntax = ["QUOTENAME(@Update_"+x+")" for x in alias_update]
        update_syntax = "+','+".join([x+"+'=_source.'+"+x for x in update_syntax])
        if include_timestamps:
            update_syntax = "+'_time_update=GETDATE(), '+"+update_syntax

        # form when not matched then insert
        insert_syntax = "+','+".join(["QUOTENAME(@Insert_"+x+")" for x in alias_insert])
        insert_values = "+','+".join(["'_source.'+QUOTENAME(@Insert_"+x+")" for x in alias_insert])
        if include_timestamps:
            insert_syntax = "+'_time_insert, '+"+insert_syntax
            insert_values = "+'GETDATE(), '+"+insert_values

        # form when not matched by source then delete condition syntax
        if delete_unmatched:
            delete_syntax = "' WHEN NOT MATCHED BY SOURCE '+{conditions_syntax}+' THEN DELETE'"
            conditions_syntax = ["'AND _target.'+QUOTENAME(@Subset_"+x+")+' IN (SELECT '+QUOTENAME(@Subset_"+x+")+' FROM '+QUOTENAME(@TableTemp)+')'" for x in alias_conditions]
            conditions_syntax = " + ".join(conditions_syntax)
            delete_syntax = delete_syntax.format(conditions_syntax=conditions_syntax)
        else:
            delete_syntax = "''"

        # parameters for sp_executesql
        parameters = ["@Match_"+x+" SYSNAME" for x in alias_match]
        parameters += ["@Update_"+x+" SYSNAME" for x in alias_update]
        parameters += ["@Insert_"+x+" SYSNAME" for x in alias_insert]
        parameters += ["@Subset_"+x+" SYSNAME" for x in alias_conditions]
        parameters =  ", ".join(parameters)

        # values for sp_executesql
        values = ["@Match_"+x+"=@Match_"+x for x in alias_match]
        values += ["@Update_"+x+"=@Update_"+x for x in alias_update]
        values += ["@Insert_"+x+"=@Insert_"+x for x in alias_insert]
        values += ["@Subset_"+x+"=@Subset_"+x for x in alias_conditions]
        values =  ", ".join(values)

        # set final SQL string
        statement = statement.format(
            declare=declare,
            match_syntax=match_syntax,
            update_syntax=update_syntax,
            insert_syntax=insert_syntax,
            insert_values=insert_values,
            delete_syntax=delete_syntax,
            parameters=parameters,
            values=values
        )

        # perform merge
        if delete_conditions is None:
            args = [table_name, table_temp]+match_columns+update_columns+insert_columns
        else:
            args = [table_name, table_temp]+match_columns+update_columns+insert_columns+delete_conditions

        # execute statement, and potentially handle errors
        self.__attempt_write(table_name, dataframe, self.__connection__.cursor.execute, statement, args)
        table_temp = helpers.safe_sql(self.__connection__, table_temp)
        self.__connection__.cursor.execute('DROP TABLE '+table_temp)
        self.__connection__.cursor.commit()


    def __attempt_write(self, table_name, dataframe, cursor_method, statement, args: list = None):
        '''Execute a statement using a pyodbc.cursor method until all built in methods to handle errors
        have been exhausted. Raises general errors to prevent exposing injection attempts.

        Parameters
        ----------

        table_name (str) : name of the SQL table
        dataframe (pandas.DataFrame): tabular data that is being written to an SQL table
        cursor_method (pyodbc.connection.cursor.execute|pyodbc.connection.cursor.executemany) : cursor method used to write data
        statement (str) : statement to execute
        args (list|None) : arguments to pass to cursor_method when executing statement, if None build from dataframe values

        Returns
        -------
        None

        '''
        # derive args from dataframe values
        derive = False
        if args is None:
            derive = True

        for idx in range(0,self.adjust_sql_attempts,1):
            error_class = errors.SQLGeneral("Generic SQL error in write.__attempt_write")
            try:
                # prepare values for writting to SQL
                dataframe = self.__prepare_values(dataframe)
                # derive each loop incase handling error adjusted dataframe contents
                if derive:
                    args = dataframe.values.tolist()
                # call cursor.execute/cursor.executemany
                cursor_method(statement, args)
                error_class = None
                break
            except (pyodbc.ProgrammingError, pyodbc.DataError, pyodbc.IntegrityError) as odbc_error:
                self.__connection__.cursor.rollback()
                error_class, undefined_columns = self.__classify_error(table_name, dataframe, odbc_error)
                dataframe, error_class = self.__handle_error(table_name, dataframe, error_class, undefined_columns)
                if error_class is not None:
                    raise error_class from None
            except errors.SQLInvalidDataType as error_class:
                raise error_class
            except Exception:
                # unclassified error
                self.__connection__.cursor.rollback()
                raise error_class from None
        # raise exception that can't be handled
        if error_class is not None:
            raise error_class from None
        # max adjust_sql_attempts reached
        if idx==self.adjust_sql_attempts-1:
            raise RecursionError(f'adjust_sql_attempts={self.adjust_sql_attempts} reached')

        self.__connection__.cursor.commit()


    def __classify_error(self, table_name: str, dataframe: pd.DataFrame, odbc_error: pyodbc.Error):
        '''Classify an ODBC write error so it can be handled
        
        Parameters
        ----------
        table_name (str) : name of table to adjust
        dataframe (pandas.DataFrame): tabular data that is being written to an SQL table
        odbc_error (pyodbc.Error) : a pyodbc error

        Returns
        -------

        error_class (mssql_dataframe.core.errors) : classified error
        undefined_columns (list) : columns that are not defined in SQL table
        '''

        error_string = str(odbc_error)
        undefined_columns = []
        
        if 'Invalid object name' in error_string:
            error_class =  errors.SQLTableDoesNotExist(f"{table_name} does not exist")
        elif 'Invalid column name' in error_string:
            undefined_columns = re.findall(r"Invalid column name '(.+?)'", error_string)
            error_class =  errors.SQLColumnDoesNotExist(f"Columns {undefined_columns} do not exist in {table_name}")
        elif 'String data, right truncation' in error_string or 'String or binary data would be truncated' in error_string:
            # additionally check schema for better error classification
            try:
                schema = helpers.get_schema(self.__connection__, table_name)
                columns, _, _, _ = helpers.flatten_schema(schema)
                _ = helpers.dtype_py(dataframe.select_dtypes('object'), columns)
                error_class = errors.SQLInsufficientColumnSize(f"A string column in {table_name} has insufficient size.")
            except errors.SQLTableDoesNotExist as error:
                # BUG: https://github.com/mkleehammer/pyodbc/issues/940
                error_class = error
            except errors.SQLInvalidDataType as error:
                # example: string data is not being written to non-string column
                raise error
        elif 'Numeric value out of range' in error_string or 'Arithmetic overflow error' in error_string:
            error_class = errors.SQLInsufficientColumnSize(f"A numeric column in {table_name} has insuffcient size.")
        elif 'Invalid character value for cast specification' in error_string or 'Restricted data type attribute violation' in error_string:
            error_class = errors.SQLInvalidInsertFormat(f"A column in {table_name} is incorrectly formatted for insert.")
        elif isinstance(odbc_error, pyodbc.IntegrityError):
            # allowable visible exception for attempt to insert duplicated primary key value
            error_class = odbc_error
        else:
            error_class = errors.SQLGeneral("Generic SQL error in write.__classify_error")

        return error_class, undefined_columns


    def __handle_error(self, table_name: str, dataframe: pd.DataFrame, error_class: errors, undefined_columns: list):
        ''' Handle an SQL write error by rasing an appropriate exeception or adjusting the SQL table. If adjust_sql_objects==True,
        the table may be created or columns may be added or modified.

        Parameters
        ----------

        table_name (str) : name of table to adjust
        dataframe (pandas.DataFrame) : tabular data that was attempted to be written
        error_class (mssql_dataframe.core.errors) : classified error
        undefined_columns (list) : columns that are not defined in SQL table

        Returns
        -------
        dataframe (pandas.DataFrame) : data that may have been modified if table was created
        error_class (mssql_dataframe.core.errors|None) : None if error was handled

        '''

        # always add include_timestamps columns
        include_timestamps = [x for x in undefined_columns if x in ['_time_update', '_time_insert']]
        if include_timestamps:
            error_class = None
            for column in include_timestamps:
                warnings.warn('Creating column {} in table {} with data type DATETIME.'.format(column, table_name), errors.SQLObjectAdjustment)
                self.__modify__.column(table_name, modify='add', column_name=column, data_type='DATETIME')

        # # convert to Python type based on SQL type
        elif isinstance(error_class, errors.SQLInvalidInsertFormat):
            error_class = None
            schema = helpers.get_schema(self.__connection__, table_name)
            columns, _, _, _ = helpers.flatten_schema(schema)
            dataframe = helpers.dtype_py(dataframe, columns)

        # raise error since adjust_sql_objects==False
        elif not isinstance(error_class, errors.SQLGeneral) and not self.adjust_sql_objects:
            error_class.args = (error_class.args[0],'Initialize with parameter adjust_sql_objects=True to create/modify SQL objects.')
            raise error_class from None

        # handle error since adjust_sql_objects==True
        else:

            # SQLTableDoesNotExist
            # # create table
            if isinstance(error_class, errors.SQLTableDoesNotExist):
                error_class = None
                warnings.warn('Creating table {}'.format(table_name), errors.SQLObjectAdjustment)
                dataframe = self.__create__.table_from_dataframe(table_name, dataframe, primary_key='infer')

            # SQLColumnDoesNotExist
            # # create missing columns
            elif isinstance(error_class, errors.SQLColumnDoesNotExist):
                error_class = None
                schema = helpers.get_schema(self.__connection__, table_name)
                table_temp = "##write_new_column_"+table_name
                new = dataframe.columns[~dataframe.columns.isin(schema.index)]
                dtypes_sql = helpers.infer_datatypes(self.__connection__, table_temp, dataframe[new])
                for column, data_type in dtypes_sql.items():
                    # warn if not a global temporary table for update/merge operations
                    if not table_name.startswith("##__update") and not table_name.startswith("##__merge"):
                        warnings.warn('Creating column {} in table {} with data type {}.'.format(column, table_name, data_type), errors.SQLObjectAdjustment)
                    self.__modify__.column(table_name, modify='add', column_name=column, data_type=data_type, not_null=False)
                # drop intermediate temp table
                table_temp = helpers.safe_sql(self.__connection__, table_temp)
                self.__connection__.cursor.execute('DROP TABLE '+table_temp)
                self.__connection__.cursor.commit()

            # SQLInsufficientColumnSize
            # # change data type and/or size (ex: tinyint to int or varchar(1) to varchar(2))
            elif isinstance(error_class, errors.SQLInsufficientColumnSize):
                error_class = None
                schema = helpers.get_schema(self.__connection__, table_name)
                table_temp = "##write_alter_column_"+table_name
                dtypes_sql = helpers.infer_datatypes(self.__connection__, table_temp, dataframe)
                columns, not_null, primary_key_column, _ = helpers.flatten_schema(schema)
                adjust = {k:v for k,v in dtypes_sql.items() if v!=columns[k]}
                for column, data_type in adjust.items():
                    # warn if not a global temporary table for update/merge operations
                    if not table_name.startswith("##__update") and not table_name.startswith("##__merge"):
                        warnings.warn('Altering column {} in table {} from data type {} to {}.'.format(column, table_name, columns[column], data_type), errors.SQLObjectAdjustment)
                    is_nullable = column in not_null
                    if column==primary_key_column:
                        # get primary key name
                        primary_key_name, primary_key_column = helpers.get_pk_details(self.__connection__, table_name)
                        # drop primary key constraint
                        self.__modify__.primary_key(table_name, modify='drop', columns=primary_key_column, primary_key_name=primary_key_name)
                        # alter column
                        self.__modify__.column(table_name, modify='alter', column_name=column, data_type=data_type, not_null=True)
                        # readd primary key constrain
                        self.__modify__.primary_key(table_name, modify='add', columns=primary_key_column, primary_key_name=primary_key_name)
                    else:
                        self.__modify__.column(table_name, modify='alter', column_name=column, data_type=data_type, not_null=is_nullable)
                # drop intermediate temp table
                table_temp = helpers.safe_sql(self.__connection__, table_temp)
                self.__connection__.cursor.execute('DROP TABLE '+table_temp)
                self.__connection__.cursor.commit()

        
        return dataframe, error_class


    def __prepare_values(self, dataframe):
        """Prepare values for writing to SQL.
        
        Parameters
        ----------

        dataframe (pandas.DataFrame) : contains values in a format that will cause inserts to fails
        table_name ()

        Returns
        -------

        DataFrame (pandas.DataFrame) : values to insert into SQL

        """

        # strings: treat empty as None
        columns = (dataframe.applymap(type) == str).all(0)
        columns = columns.index[columns]
        dataframe[columns] = dataframe[columns].replace(r'^\s*$', np.nan, regex=True)

        # timedetlas: convert to string
        columns = list(dataframe.select_dtypes('timedelta').columns)
        if columns:
            invalid = ((dataframe[columns]>=pd.Timedelta(days=1)) | (dataframe[columns]<pd.Timedelta(days=0))).any()
            if any(invalid):
                invalid = list(invalid[invalid].index)
                raise ValueError(f'columns {invalid} are out of range for time data type. Allowable range is 00:00:00.0000000-23:59:59.9999999')
            dataframe[columns] = dataframe[columns].astype('str')
            dataframe[columns] = dataframe[columns].apply(lambda x: x.str[7:23])

        # any kind of missing values to be NULL in SQL
        dataframe = dataframe.fillna(np.nan).replace([np.nan], [None])

        # datetimes: convert dataframe of single datetime column
        # # otherwise dataframe.values.tolist() will then be composed of Python int's instead of Python Timestamps
        if dataframe.shape[1]==1 and dataframe.select_dtypes('datetime').shape[1]==1:
            dataframe = dataframe.astype(object)

        return dataframe


    def __prep_update_merge(self, table_name, match_columns, dataframe, operation: Literal['update','merge']):

        if isinstance(match_columns,str):
            match_columns = [match_columns]

        # read target table schema
        schema = helpers.get_schema(self.__connection__, table_name)

        # check validitiy of match_columns, use primary key if needed
        if match_columns is None:
            match_columns = list(schema[schema['is_primary_key']].index)
            if not match_columns:
                raise errors.SQLUndefinedPrimaryKey('SQL table {} has no primary key. Either set the primary key or specify the match_columns'.format(table_name))
        # check match_column presence is SQL table
        if sum(schema.index.isin(match_columns))!=len(match_columns):
            raise errors.SQLColumnDoesNotExist('one of match_columns {} is not found in SQL table {}'.format(match_columns,table_name))
        # check match_column presence in dataframe, use dataframe index if needed
        if any(dataframe.index.names):
            dataframe = dataframe.reset_index()
        if sum(dataframe.columns.isin(match_columns))!=len(match_columns):
            raise errors.DataframeUndefinedColumn('one of match_columns {} is not found in the input dataframe'.format(match_columns))

        # check for new columns instead of relying on _attempt_write to prevent error for both temp table and target table
        undefined_columns = list(dataframe.columns[~dataframe.columns.isin(schema.index)])
        if any(undefined_columns):
            error_class = errors.SQLColumnDoesNotExist(f'Invalid column name: {undefined_columns}')
            dataframe,_ = self.__handle_error(table_name, dataframe, error_class, undefined_columns)
            schema = helpers.get_schema(self.__connection__, table_name)

        # insert data into temporary table to use for updating/merging
        table_temp = "##__"+operation+"_"+table_name
        temp = schema[schema.index.isin(list(dataframe.columns)+[dataframe.index.name])]
        columns, not_null, primary_key_column, _ = helpers.flatten_schema(temp)
        self.__create__.table(table_temp, columns, not_null, primary_key_column, sql_primary_key=False)
        self.insert(table_temp, dataframe, include_timestamps=False)

        return dataframe, match_columns, table_temp

