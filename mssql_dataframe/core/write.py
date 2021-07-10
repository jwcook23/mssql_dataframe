from typing import Literal
import warnings

import pandas as pd
import numpy as np
import pyodbc

from mssql_dataframe.core import errors, helpers, create, modify


class write():

    def __init__(self, connection):
        '''Class for writing to SQL tables.
        
        Parameters
        ----------
        connection (mssql_dataframe.connect) : connection for executing statement
        '''

        self.__connection__ = connection
        self.__create__ = create.create(connection)
        self.__modify__ = modify.modify(connection)


    def insert(self, table_name: str, dataframe: pd.DataFrame, 
    create_table: bool = True, add_column: bool = True, alter_column: bool = True):
        """Insert data into SQL table from a dataframe.

        Parameters
        ----------

        table_name (str) : name of table to insert data into
        dataframe (pd.DataFrame): tabular data to insert
        create_table (bool, default=True) : if table doesn't exist, create it
        add_column (bool, default=True) : if column doesn't exist, create it
        alter_column (bool, default=True) : if column data type and size don't allow insert, adjust it

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
        values = dataframe.values.tolist()
        try:
            self.__connection__.cursor.executemany(statement, values)
        except pyodbc.ProgrammingError as error:
            if 'Invalid object name' in str(error):
                if create_table:
                    warnings.warn("Attempt to insert into table that does not exist. Creating table first as parameter create_table=True", errors.SQLObjectCreation)
                    _ = self.__create__.table_from_dataframe(table_name, dataframe)
                    self.__connection__.cursor.executemany(statement, values)
                else:
                    raise errors.SQLTableDoesNotExist("{table_name} does not exist".format(table_name=table_name)) from None
            elif 'Invalid column name' in str(error):
                raise errors.SQLColumnDoesNotExist("Column does not exist in {table_name}".format(table_name=table_name)) from None
            elif 'String data, right truncation' in str(error):
                raise errors.SQLInsufficientColumnSize("A string column in {table_name} has insuffcient size to insert values.".format(table_name=table_name)) from None
            else:
                raise errors.SQLGeneral("SQLGeneral") from None
        except pyodbc.DataError as error:
            raise errors.SQLInsufficientColumnSize("A numeric column in {table_name} has insuffcient size to insert values.".format(table_name=table_name)) from None
        except Exception as error:
            raise errors.SQLGeneral("Generic error attempting to insert values.")


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

        # perform common pre-update/merge steps
        dataframe, match_columns, table_temp = self.__prep_update_merge(table_name, match_columns, dataframe, operation='update')

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
        # ' ON _table.'+QUOTENAME(@PrimaryKey)+'=_temp.'+QUOTENAME(@PrimaryKey)+';'
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
        helpers.execute(self.__connection__, statement, args)


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

        # perform common pre-update/merge steps
        dataframe, match_columns, table_temp = self.__prep_update_merge(table_name, match_columns, dataframe, operation='merge')

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
        helpers.execute(self.__connection__, statement, args)


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


    def __new_column(self, table_name: str, dataframe: pd.DataFrame, column_names: list):
        '''Add new column(s) to table after insert failure.
        
        Parameters
        ----------

        table_name (str) : name of table to add column(s)
        dataframe (pandas.DataFrame) : data containing columns(s) to add
        column_names (list) : new column(s) to add

        Returns
        -------
        None
        '''

        table_temp = "##__new_column_"+table_name

        dtypes = helpers.infer_datatypes(self.__connection__, table_temp, dataframe[column_names])

        for column, data_type in dtypes.items():
            # SQL does not allow adding a non-null column
            self.__modify__.column(table_name, modify='add', column_name=column, data_type=data_type, not_null=False)


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
            raise errors.SQLUndefinedColumn('match_columns {} is not found in SQL table {}'.format(match_columns,table_name))
        # check match_column presence in dataframe, use dataframe index if needed
        if sum(dataframe.columns.isin(match_columns))!=len(match_columns):
            if len([x for x in match_columns if x==dataframe.index.name])>0:
                dataframe = dataframe.reset_index()
            else:
                raise errors.DataframeUndefinedColumn('match_columns {} is not found in the input dataframe'.format(match_columns))

        # check if new columns need to be added to SQL table
        new = dataframe.columns[~dataframe.columns.isin(schema.index)]
        if len(new)>0:
            self.__new_column(table_name, dataframe.reset_index(drop=True), column_names=new)
            schema = helpers.get_schema(self.__connection__, table_name)
        temp = schema[schema.index.isin(list(dataframe.columns)+[dataframe.index.name])]
        columns, not_null, primary_key_column, _ = self.__create__._create__table_schema(temp)

        # add interal tracking columns if needed
        if operation=='merge' and '_time_insert' not in schema.index:
            self.__modify__.column(table_name, modify='add', column_name='_time_insert', data_type='DATETIME')
        if '_time_update' not in schema.index:
            self.__modify__.column(table_name, modify='add', column_name='_time_update', data_type='DATETIME')

        # insert data into temporary table to use for updating/merging
        table_temp = "##"+operation+"_"+table_name
        self.__create__.table(table_temp, columns, not_null, primary_key_column, sql_primary_key=False)
        self.insert(table_temp, dataframe)

        return dataframe, match_columns, table_temp

