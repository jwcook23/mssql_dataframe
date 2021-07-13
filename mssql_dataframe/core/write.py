from typing import Literal
import warnings

import pandas as pd
import numpy as np
import pyodbc

from mssql_dataframe.core import errors, helpers, create, modify


class write():

    def __init__(self, connection, adjust_sql_objects: bool = False):
        '''Class for writing to SQL tables.
        
        Parameters
        ----------
        connection (mssql_dataframe.connect) : connection for executing statement
        adjust_sql_objects (bool) : create and modify SQL table and columns as needed if True

        '''

        self.__connection__ = connection
        self.__create__ = create.create(connection)
        self.__modify__ = modify.modify(connection)
        self.adjust_sql_objects = adjust_sql_objects


    def insert(self, table_name: str, dataframe: pd.DataFrame):
        """Insert data into SQL table from a dataframe.

        Parameters
        ----------

        table_name (str) : name of table to insert data into
        dataframe (pd.DataFrame): tabular data to insert

        Returns
        -------
        
        None

        Examples
        --------

        insert(connection, 'TableName', pd.DataFrame({'ColumnA': [1, 2, 3]}))

        """

        # write index column as it is the primary key
        if dataframe.index.name is not None:
            dataframe = dataframe.reset_index()

        # sanitize table and column names for safe sql
        table_clean = helpers.safe_sql(self.__connection__, table_name)
        column_names = ",\n".join(helpers.safe_sql(self.__connection__, dataframe.columns))

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
            parameters=', '.join(['?']*len(dataframe.columns))
        )
        dataframe = self.__prepare_values(dataframe)
        args = dataframe.values.tolist()
        try:
            self.__connection__.cursor.executemany(statement, args)
        except (pyodbc.ProgrammingError, pyodbc.DataError) as error_class:
            self.__handle_error(table_name, dataframe, error_class)
            self.__connection__.cursor.executemany(statement, args)
        except Exception:
            raise errors.SQLGeneral("Generic SQL error in write.insert") from None


    def update(self, table_name: str, dataframe: pd.DataFrame, match_columns: list = None):
        """Update column(s) in an SQL table using a dataframe.

        Parameters
        ----------

        table_name (str) : name of table to insert data into
        dataframe (pd.DataFrame): tabular data to insert
        match_columns (list, default=None) : matches records between dataframe and SQL table, if None the SQL primary key is used

        Returns
        -------
        
        None

        Examples
        --------

        table_name = "##test_update_performance"

        dataframe = pd.DataFrame({
            'ColumnA': [0]*100000
        })
        create.table_from_dataframe(connection, table_name, dataframe, primary_key='index', row_count=len(dataframe))

        # update values in table
        dataframe['ColumnA'] = list(range(0,100000,1))
        write.update(connection, table_name, dataframe[['ColumnA']])
        

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
                    '_time_update=GETDATE(),'+{update_syntax}+
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
        try:
            self.__connection__.cursor.execute(statement, args)
        except (pyodbc.ProgrammingError, pyodbc.DataError) as error_class:
            self.__handle_error(table_name, dataframe, error_class)
            self.__connection__.cursor.execute(statement, args)
        except Exception:
            raise errors.SQLGeneral("Generic SQL error in write.update") from None


    def merge(self, table_name: str, dataframe: pd.DataFrame, match_columns: list = None, subset_columns: list = None):
        ''' Merge a dataframe into an SQL table by updating, deleting, and inserting rows using Transact-SQL MERGE.

        Parameters
        ----------

        table_name (str) : name of the SQL table
        dataframe (pd.DataFrame): tabular data to merge into SQL table
        match_columns (list, default=None) : matches records between dataframe and SQL table, if None the SQL primary key is used
        subset_columns (list, default=None) : prevents deleting non-matching columns during incremental loading

        Returns
        -------
        
        None

        '''

        # perform pre-merge steps
        try:
            dataframe, match_columns, table_temp = self.__prep_update_merge(table_name, match_columns, dataframe, operation='merge')
        except errors.SQLTableDoesNotExist:
            error_class = pyodbc.ProgrammingError('Invalid object name')
            self.__handle_error(table_name, dataframe, error_class)
            self.insert(table_name, dataframe)
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
            +' WHEN MATCHED THEN UPDATE SET _time_update=GETDATE(), '+{update_syntax}
            +' WHEN NOT MATCHED THEN INSERT (_time_insert, '+{insert_syntax}+')'
            +' VALUES (GETDATE(), '+{insert_values}+')'
            +' WHEN NOT MATCHED BY SOURCE '+{subset_syntax}+' THEN DELETE;'

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
        if subset_columns is None:
            alias_subset = []
        else:
            alias_subset = [str(x) for x in list(range(0,len(subset_columns)))]

        # declare SQL variables
        declare = ["DECLARE @Match_"+x+" SYSNAME = ?;" for x in alias_match]
        declare += ["DECLARE @Update_"+x+" SYSNAME = ?;" for x in alias_update]
        declare += ["DECLARE @Insert_"+x+" SYSNAME = ?;" for x in alias_insert]
        declare += ["DECLARE @Subset_"+x+" SYSNAME = ?;" for x in alias_subset]
        declare = "\n".join(declare)

        # form match on syntax
        match_syntax = ["QUOTENAME(@Match_"+x+")" for x in alias_match]
        match_syntax = "+' AND '+".join(["'_target.'+"+x+"+'=_source.'+"+x for x in match_syntax])

        # form when matched then update syntax
        update_syntax = ["QUOTENAME(@Update_"+x+")" for x in alias_update]
        update_syntax = "+','+".join([x+"+'=_source.'+"+x for x in update_syntax])

        # form when not matched then insert
        insert_syntax = "+','+".join(["QUOTENAME(@Insert_"+x+")" for x in alias_insert])
        insert_values = "+','+".join(["'_source.'+QUOTENAME(@Insert_"+x+")" for x in alias_insert])

        # form when not matched by source then delete condition syntax
        if subset_columns is None:
            subset_syntax = "''"
        else:
            subset_syntax = ["'AND _target.'+QUOTENAME(@Subset_"+x+")+' IN (SELECT '+QUOTENAME(@Subset_"+x+")+' FROM '+QUOTENAME(@TableTemp)+')'" for x in alias_subset]
            subset_syntax = " + ".join(subset_syntax)

        # parameters for sp_executesql
        parameters = ["@Match_"+x+" SYSNAME" for x in alias_match]
        parameters += ["@Update_"+x+" SYSNAME" for x in alias_update]
        parameters += ["@Insert_"+x+" SYSNAME" for x in alias_insert]
        parameters += ["@Subset_"+x+" SYSNAME" for x in alias_subset]
        parameters =  ", ".join(parameters)

        # values for sp_executesql
        values = ["@Match_"+x+"=@Match_"+x for x in alias_match]
        values += ["@Update_"+x+"=@Update_"+x for x in alias_update]
        values += ["@Insert_"+x+"=@Insert_"+x for x in alias_insert]
        values += ["@Subset_"+x+"=@Subset_"+x for x in alias_subset]
        values =  ", ".join(values)

        # set final SQL string
        statement = statement.format(
            declare=declare,
            match_syntax=match_syntax,
            update_syntax=update_syntax,
            insert_syntax=insert_syntax,
            insert_values=insert_values,
            subset_syntax=subset_syntax,
            parameters=parameters,
            values=values
        )

        # perform merge
        if subset_columns is None:
            args = [table_name, table_temp]+match_columns+update_columns+insert_columns
        else:
            args = [table_name, table_temp]+match_columns+update_columns+insert_columns+subset_columns
        try:
            self.__connection__.cursor.execute(statement, args)
        except (pyodbc.ProgrammingError, pyodbc.DataError) as error_class:
            self.__handle_error(table_name, dataframe, error_class)
            self.__connection__.cursor.execute(statement, args)
        except Exception:
            raise errors.SQLGeneral("General SQL error in write.merge") from None


    def __handle_error(self, table_name: str, dataframe: pd.DataFrame, error_class: pyodbc.Error):
        ''' Handle an SQL write error by rasing an appropriate exeception or adjusting the SQL table. If adjust_sql_objects==True,
        the table may be created or columns may be added or modified.

        Parameters
        ----------

        table_name (str) : name of table to adjust
        dataframe (pandas.DataFrame) : tabular data to compare against SQL table
        error_class (pyodbc.Error) : a pyodbc error

        Returns
        -------
        None

        '''

        # determine class of error
        error_string = str(error_class)
        if 'Invalid object name' in error_string:
            error_message =  errors.SQLTableDoesNotExist("{table_name} does not exist".format(table_name=table_name))
        elif 'Invalid column name' in error_string:
            error_message =  errors.SQLColumnDoesNotExist("Column does not exist in {table_name}".format(table_name=table_name))
        elif 'String data, right truncation' in error_string or 'String or binary data would be truncated' in error_string:
            error_message = errors.SQLInsufficientColumnSize("A string column in {table_name} has insuffcient size.".format(table_name=table_name))
        elif 'Numeric value out of range' in error_string or 'Arithmetic overflow error' in error_string:
            error_message = errors.SQLInsufficientColumnSize("A numeric column in {table_name} has insuffcient size.".format(table_name=table_name))
        else:
            error_message = errors.SQLGeneral("Generic SQL error in write.__handle_error")
        
        # raise or handle error
        if not self.adjust_sql_objects:
            error_message.args = (error_message.args[0],'Initialize with parameter adjust_sql_objects=True to create/modify SQL objects.')
            raise error_message from None
        else:
            if isinstance(error_message, errors.SQLTableDoesNotExist):
                warnings.warn('Creating table {}'.format(table_name), errors.SQLObjectAdjustment)
                self.__create__.table_from_dataframe(table_name, dataframe)
            else:
                schema = helpers.get_schema(self.__connection__, table_name)
                if isinstance(error_message, errors.SQLColumnDoesNotExist):
                    table_temp = "##__new_column_"+table_name
                    new = dataframe.columns[~dataframe.columns.isin(schema.index)]
                    dtypes = helpers.infer_datatypes(self.__connection__, table_temp, dataframe[new])
                    for column, data_type in dtypes.items():
                        warnings.warn('Creating column {} in table {} with data type {}.'.format(column, table_name, data_type), errors.SQLObjectAdjustment)
                        self.__modify__.column(table_name, modify='add', column_name=column, data_type=data_type, not_null=False)
                elif isinstance(error_message, errors.SQLInsufficientColumnSize):
                    table_temp = "##__alter_column_"+table_name
                    dtypes = helpers.infer_datatypes(self.__connection__, table_temp, dataframe)
                    columns, not_null, _,_ = helpers.flatten_schema(schema)
                    adjust = {k:v for k,v in dtypes.items() if v!=columns[k]}
                    for column, data_type in adjust.items():
                        warnings.warn('Altering column {} in table {} to {}.'.format(column, table_name, data_type), errors.SQLObjectAdjustment)
                        is_nullable = column in not_null
                        self.__modify__.column(table_name, modify='alter', column_name=column, data_type=data_type, not_null=is_nullable)
                else:
                    raise error_message from None


    def __prepare_values(self, dataframe):
        """Prepare values for loading into SQL.
        
        Parameters
        ----------

        dataframe (pandas.DataFrame) : contains NA values

        Returns
        -------

        DataFrame (pandas.DataFrame) : NA values changed to None

        """

        # strip leading/trailing spaces and empty strings
        columns = (dataframe.applymap(type) == str).all(0)
        columns = columns.index[columns]
        dataframe[columns] = dataframe[columns].apply(lambda x: x.str.strip())
        dataframe[columns] = dataframe[columns].replace(r'^\s*$', np.nan, regex=True)

        # missing values as None, to be NULL in SQL
        dataframe = dataframe.fillna(np.nan).replace([np.nan], [None])

        return dataframe


    def __prep_update_merge(self, table_name, match_columns, dataframe, operation: Literal['update','merge']):

        if isinstance(match_columns,str):
            match_columns = [match_columns]

        # read target table schema
        schema = helpers.get_schema(self.__connection__, table_name)

        # check validitiy of match_columns, use primary key if needed
        if match_columns is None:
            match_columns = list(schema[schema['is_primary_key']].index)
            if len(match_columns)==0:
                raise errors.SQLUndefinedPrimaryKey('SQL table {} has no primary key. Either set the primary key or specify the match_columns'.format(table_name))
        # check match_column presence is SQL table
        if sum(schema.index.isin(match_columns))!=len(match_columns):
            raise errors.SQLUndefinedColumn('one of match_columns {} is not found in SQL table {}'.format(match_columns,table_name))
        # check match_column presence in dataframe, use dataframe index if needed
        if sum(dataframe.columns.isin(match_columns))!=len(match_columns):
            if len([x for x in match_columns if x==dataframe.index.name])>0:
                dataframe = dataframe.reset_index()
            else:
                raise errors.DataframeUndefinedColumn('match_columns {} is not found in the input dataframe'.format(match_columns))

        # check if new columns need to be added to SQL table
        if any(~dataframe.columns.isin(schema.index)):
            error_class = pyodbc.ProgrammingError('Invalid column name')
            self.__handle_error(table_name, dataframe, error_class)
            schema = helpers.get_schema(self.__connection__, table_name)

        # add interal tracking columns if needed
        if operation=='merge' and '_time_insert' not in schema.index:
            self.__modify__.column(table_name, modify='add', column_name='_time_insert', data_type='DATETIME')
        if '_time_update' not in schema.index:
            self.__modify__.column(table_name, modify='add', column_name='_time_update', data_type='DATETIME')

        # insert data into temporary table to use for updating/merging
        table_temp = "##"+operation+"_"+table_name
        temp = schema[schema.index.isin(list(dataframe.columns)+[dataframe.index.name])]
        columns, not_null, primary_key_column, _ = helpers.flatten_schema(temp)
        self.__create__.table(table_temp, columns, not_null, primary_key_column, sql_primary_key=False)
        self.insert(table_temp, dataframe)

        return dataframe, match_columns, table_temp

