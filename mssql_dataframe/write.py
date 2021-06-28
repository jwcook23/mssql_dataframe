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

    # perform update using primary key
    schema = helpers.get_schema(connection, table_name)
    primary_key =  schema[schema['is_primary_key']].index
    if len(primary_key)==0:
        raise errors.UndefinedPrimaryKey("Primary key not defined in SQL table: "+table_name)
    else:
        primary_key = primary_key[0]
    
    # insert data into temporary SQL table
    if dataframe.index.name is None:
        raise errors.UndefinedPrimaryKey("Index (primary key) of the input dataframe is not defined.")
    table_temp = "##update_"+table_name
    temp = schema[schema.index.isin(list(dataframe.columns)+[dataframe.index.name])]
    columns, not_null, primary_key_column, _ = create.__table_schema(temp)
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
    declare = '\n'.join(["DECLARE @Column_"+x+" SYSNAME = ?;" for x in alias_names])
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
    args = [table_name, table_temp, primary_key] + column_names
    connection.cursor.execute(statement, *args)


def merge():
    # TODO: define merge function
    raise NotImplementedError('merge not implemented') from None
    
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