import pandas as pd
import numpy as np
import pyodbc

from mssql_dataframe import errors
from mssql_dataframe import helpers
from mssql_dataframe import create
from mssql_dataframe import modify


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

    # dataframe index is likely the SQL primary key
    if dataframe.index.name is not None:
        dataframe = dataframe.reset_index()

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
        if 'not sure what this error should be' in str(error):
            raise errors.TableDoesNotExist("{table_name} does not exist".format(table_name=table_name)) from None
        elif 'Invalid column name' in str(error):
            raise errors.ColumnDoesNotExist("Column does not exist in {table_name}".format(table_name=table_name)) from None
        else:
            raise errors.GeneralError("GeneralError") from None
    except pyodbc.DataError:
        raise errors.InsufficientColumnSize("A column in {table_name} has insuffcient size to insert values.".format(table_name=table_name)) from None
    except:
        raise errors.GeneralError("GeneralError") from None


def update(connection, table_name, dataframe):
    """Update column(s) in an SQL table using a dataframe using the SQL table's primary key.

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

    

    """

    # insert data into temporary SQL table
    table_temp = "##update_"+table_name
    if dataframe.index.name is None:
        raise errors.UndefinedPrimaryKey("Index (primary key) of the input dataframe is not defined.")
    schema = helpers.get_schema(connection, table_name)
    new = dataframe.columns[~dataframe.columns.isin(schema.index)]
    if len(new)>0:
        new_column(connection, table_name, dataframe.reset_index(drop=True), column_names=new)
        schema = helpers.get_schema(connection, table_name)
    temp = schema[schema.index.isin(list(dataframe.columns)+[dataframe.index.name])]
    columns, not_null, primary_key_column, sql_primary_key = create.__table_schema(temp)
    if primary_key_column is None and sql_primary_key==False:
        raise errors.UndefinedPrimaryKey("Primary key not defined in SQL table: "+table_name)
    elif sql_primary_key:
        primary_key_column = '_pk'
    # # sql_primary_key == False since it must be input to this function
    create.table(connection, table_temp, columns, not_null, primary_key_column, sql_primary_key=False)
    insert(connection, table_temp, dataframe)

    statement = """
        DECLARE @SQLStatement AS NVARCHAR(MAX);
        DECLARE @TableName SYSNAME = ?;
        DECLARE @TableTemp SYSNAME = ?;
        DECLARE @PrimaryKey SYSNAME = ?;
        {declare}

        SET @SQLStatement = 
            N'UPDATE '+
                QUOTENAME(@TableName)+
            ' SET '+ 
                {columns}+
            ' FROM '+
                QUOTENAME(@TableName)+' AS _table '+
            ' INNER JOIN '+
                QUOTENAME(@TableTemp)+' AS _temp '+
                ' ON _table.'+QUOTENAME(@PrimaryKey)+'=_temp.'+QUOTENAME(@PrimaryKey)+';'
        EXEC sp_executesql 
            @SQLStatement,
            N'@TableName SYSNAME, @TableTemp SYSNAME, @PrimaryKey SYSNAME, {parameters}',
            @TableName=@TableName, @TableTemp=@TableTemp, @PrimaryKey=@PrimaryKey, {values};
    """

    column_names = list(dataframe.columns)
    alias_names = [str(x) for x in list(range(0,len(column_names)))]
    declare = "\n".join(["DECLARE @Column_"+x+" SYSNAME = ?;" for x in alias_names])
    columns = ["QUOTENAME(@Column_"+x+")" for x in alias_names]
    columns = ",\n".join([x+"+'=_temp.'+"+x for x in columns])
    parameters = ", ".join(["@Column_"+x+" SYSNAME" for x in alias_names])
    values = ", ".join(["@Column_"+x+"=@Column_"+x for x in alias_names])

    statement = statement.format(
        declare=declare,
        columns=columns,
        parameters=parameters,
        values=values
    )

    # perform update
    args = [table_name, table_temp, primary_key_column] + column_names
    connection.cursor.execute(statement, *args)


def merge(connection, table_name: str, dataframe: pd.DataFrame, match_columns: list, subset_columns: list = None):
    ''' Merge a dataframe into an SQL table by updating, deleting, and inserting rows using Transact-SQL MERGE.

    Parameters
    ----------

    connection (mssql_dataframe.connect) : connection for executing statement
    table_name (str) : name of the SQL table
    match_columns (list) : column names used to match records between the dataframe and SQL table
    subset_columns (list) : used to prevent deleting non-matching columns during incremental loading
    dataframe (pd.DataFrame): tabular data to merge into SQL table

    Returns
    -------
    
    None

    '''

    # read target table schema
    schema = helpers.get_schema(connection, table_name)

    # add new columns if needed
    new = dataframe.columns[~dataframe.columns.isin(schema.index)]
    if len(new)>0:
        new_column(connection, table_name, dataframe.reset_index(drop=True), column_names=new)
        schema = helpers.get_schema(connection, table_name)

    # add interal tracking columns if needed
    if '_time_insert' not in schema.index:
        modify.column(connection, table_name, modify='add', column_name='_time_insert', data_type='DATETIME')
    if '_time_update' not in schema.index:
        modify.column(connection, table_name, modify='add', column_name='_time_update', data_type='DATETIME')

    # insert data into source temporary table with same schema as target
    table_temp = "##merge_"+table_name
    columns, not_null, primary_key_column, sql_primary_key = create.__table_schema(schema)
    create.table(connection, table_temp, columns, not_null, primary_key_column, sql_primary_key)
    insert(connection, table_temp, dataframe)

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

    # update all columns in dataframe, besides match columns
    update_columns = list(dataframe.columns[~dataframe.columns.isin(match_columns)])

    # insert all columns in dataframe
    insert_columns = list(dataframe.columns)

    # alias columns to prevent direct input into SQL syntax
    alias_match = [str(x) for x in list(range(0,len(match_columns)))]
    alias_update = [str(x) for x in list(range(0,len(update_columns)))]
    alias_insert = [str(x) for x in list(range(0,len(insert_columns)))]
    if subset_columns is None:
        alias_subset = []
    else:
        alias_subset = [str(x) for x in list(range(0,len(subset_columns)))]

    declare = ["DECLARE @Match_"+x+" SYSNAME = ?;" for x in alias_match]
    declare += ["DECLARE @Update_"+x+" SYSNAME = ?;" for x in alias_update]
    declare += ["DECLARE @Insert_"+x+" SYSNAME = ?;" for x in alias_insert]
    declare += ["DECLARE @Subset_"+x+" SYSNAME = ?;" for x in alias_subset]
    declare = "\n".join(declare)

    match_syntax = ["QUOTENAME(@Match_"+x+")" for x in alias_match]
    match_syntax = "+' AND '+".join(["'_target.'+"+x+"+'=_source.'+"+x for x in match_syntax])

    update_syntax = ["QUOTENAME(@Update_"+x+")" for x in alias_update]
    update_syntax = "+','+".join([x+"+'=_source.'+"+x for x in update_syntax])

    insert_syntax = "+','+".join(["QUOTENAME(@Insert_"+x+")" for x in alias_insert])
    insert_values = "+','+".join(["'_source.'+QUOTENAME(@Insert_"+x+")" for x in alias_insert])

    # AND _target.COUNTY IN (SELECT COUNTY FROM ##_merge_TEST)
    if subset_columns is None:
        subset_syntax = "''"
    else:
        subset_syntax = ["'AND _target.'+QUOTENAME(@Subset_"+x+")+' IN (SELECT '+QUOTENAME(@Subset_"+x+")+' FROM '+QUOTENAME(@TableTemp)+')'" for x in alias_subset]
        subset_syntax = " + ".join(subset_syntax)

    parameters = ["@Match_"+x+" SYSNAME" for x in alias_match]
    parameters += ["@Update_"+x+" SYSNAME" for x in alias_update]
    parameters += ["@Insert_"+x+" SYSNAME" for x in alias_insert]
    parameters += ["@Subset_"+x+" SYSNAME" for x in alias_subset]
    parameters =  ", ".join(parameters)

    values = ["@Match_"+x+"=@Match_"+x for x in alias_match]
    values += ["@Update_"+x+"=@Update_"+x for x in alias_update]
    values += ["@Insert_"+x+"=@Insert_"+x for x in alias_insert]
    values += ["@Subset_"+x+"=@Subset_"+x for x in alias_subset]
    values =  ", ".join(values)

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

    if subset_columns is None:
        args = [table_name, table_temp]+match_columns+update_columns+insert_columns
    else:
        args = [table_name, table_temp]+match_columns+update_columns+insert_columns+subset_columns
    connection.cursor.execute(statement, *args)