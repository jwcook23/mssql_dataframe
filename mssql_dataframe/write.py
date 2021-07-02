from typing import Literal
import pandas as pd
import numpy as np
import pyodbc

from mssql_dataframe import errors, helpers, create, modify


def prepare_values(dataframe):
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


def new_column(connection, table_name: str, dataframe: pd.DataFrame, column_names: list):
    '''Add new column(s) to table after insert failure.
    
    Parameters
    ----------
    connection (mssql_dataframe.connect) : connection for executing statement
    table_name (str) : name of table to add column(s)
    dataframe (pandas.DataFrame) : data containing columns(s) to add
    column_names (list) : new column(s) to add

    Returns
    -------
    None
    '''

    table_temp = "##new_column_"+table_name

    dtypes = helpers.infer_datatypes(connection, table_temp, dataframe[column_names])

    for column, data_type in dtypes.items():
        # SQL does not allow adding a non-null column
        modify.column(connection, table_name, modify='add', column_name=column, data_type=data_type, not_null=False)


def insert(connection, table_name: str, dataframe: pd.DataFrame):
    """Insert data into SQL table from a dataframe.

    Parameters
    ----------

    connection (mssql_dataframe.connect) : connection for executing statement
    table_name (str) : name of table to insert data into
    dataframe (pd.DataFrame): tabular data to insert

    Returns
    -------
    
    None

    Examples
    --------

    insert(connection, 'TableName', pd.DataFrame({'ColumnA': [1, 2, 3]}))

    """

    # sanitize table and column names for safe sql
    table_name = helpers.safe_sql(connection, table_name)
    column_names = ",\n".join(helpers.safe_sql(connection, dataframe.columns))

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
        table_name=table_name, 
        column_names=column_names,
        parameters=', '.join(['?']*len(dataframe.columns))
    )
    dataframe = prepare_values(dataframe)
    values = dataframe.values.tolist()
    try:
        connection.cursor.executemany(statement, values)
    except pyodbc.ProgrammingError as error:
        if 'Invalid object name' in str(error):
            raise errors.TableDoesNotExist("{table_name} does not exist".format(table_name=table_name)) from None
        elif 'Invalid column name' in str(error):
            raise errors.ColumnDoesNotExist("Column does not exist in {table_name}".format(table_name=table_name)) from None
        elif 'String data, right truncation' in str(error):
            raise errors.InsufficientColumnSize("A string column in {table_name} has insuffcient size to insert values.".format(table_name=table_name)) from None
        else:
            raise errors.GeneralError("GeneralError") from None
    except pyodbc.DataError:
        raise errors.InsufficientColumnSize("A numeric column in {table_name} has insuffcient size to insert values.".format(table_name=table_name)) from None


def __prep_update_merge(connection, table_name, match_columns, dataframe, operation: Literal['update','merge']):

    if isinstance(match_columns,str):
        match_columns = [match_columns]

   # read target table schema
    schema = helpers.get_schema(connection, table_name)

    # check validitiy of match_columns, use primary key if needed
    if match_columns is None:
        match_columns = list(schema[schema['is_primary_key']].index)
        if len(match_columns)==0:
            raise errors.UndefinedSQLPrimaryKey('SQL table {} has no primary key. Either set the primary key or specify the match_columns'.format(table_name))
    # check match_column presence is SQL table
    if sum(schema.index.isin(match_columns))!=len(match_columns):
        raise errors.UndefinedSQLColumn('match_columns {} is not found in SQL table {}'.format(match_columns,table_name))
    # check match_column presence in dataframe, use dataframe index if needed
    if sum(dataframe.columns.isin(match_columns))!=len(match_columns):
        if len([x for x in match_columns if x==dataframe.index.name])>0:
            dataframe = dataframe.reset_index()
        else:
            raise errors.UndefinedDataframeColumn('match_columns {} is not found in the input dataframe'.format(match_columns))

    # check if new columns need to be added to SQL table
    new = dataframe.columns[~dataframe.columns.isin(schema.index)]
    if len(new)>0:
        new_column(connection, table_name, dataframe.reset_index(drop=True), column_names=new)
        schema = helpers.get_schema(connection, table_name)
    temp = schema[schema.index.isin(list(dataframe.columns)+[dataframe.index.name])]
    columns, not_null, primary_key_column, _ = create.__table_schema(temp)

    # add interal tracking columns if needed
    if operation=='merge' and '_time_insert' not in schema.index:
        modify.column(connection, table_name, modify='add', column_name='_time_insert', data_type='DATETIME')
    if '_time_update' not in schema.index:
        modify.column(connection, table_name, modify='add', column_name='_time_update', data_type='DATETIME')

    # insert data into temporary table to use for updating/merging
    table_temp = "##"+operation+"_"+table_name
    create.table(connection, table_temp, columns, not_null, primary_key_column, sql_primary_key=False)
    insert(connection, table_temp, dataframe)

    return dataframe, match_columns, table_temp


def update(connection, table_name: str, dataframe: pd.DataFrame, match_columns: list = None):
    """Update column(s) in an SQL table using a dataframe.

    Parameters
    ----------

    connection (mssql_dataframe.connect) : connection for executing statement
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
    create.from_dataframe(connection, table_name, dataframe, primary_key='index', row_count=len(dataframe))

    # update values in table
    dataframe['ColumnA'] = list(range(0,100000,1))
    write.update(connection, table_name, dataframe[['ColumnA']])
    

    """

    # perform common pre-update/merge steps
    dataframe, match_columns, table_temp = __prep_update_merge(connection, table_name, match_columns, dataframe, operation='update')

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
    helpers.execute(connection, statement, args)


def merge(connection, table_name: str, dataframe: pd.DataFrame, match_columns: list = None, subset_columns: list = None):
    ''' Merge a dataframe into an SQL table by updating, deleting, and inserting rows using Transact-SQL MERGE.

    Parameters
    ----------

    connection (mssql_dataframe.connect) : connection for executing statement
    table_name (str) : name of the SQL table
    dataframe (pd.DataFrame): tabular data to merge into SQL table
    match_columns (list, default=None) : matches records between dataframe and SQL table, if None the SQL primary key is used
    subset_columns (list, default=None) : prevents deleting non-matching columns during incremental loading

    Returns
    -------
    
    None

    '''

    # perform common pre-update/merge steps
    dataframe, match_columns, table_temp = __prep_update_merge(connection, table_name, match_columns, dataframe, operation='merge')

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
    helpers.execute(connection, statement, args)