'''Functions from managing write operation attemps.'''
from typing import Literal
import warnings
import re

import pandas as pd
import pyodbc

from mssql_dataframe import errors, conversion

def _precheck(table_name, dataframe):
    '''Check the contents of a dataframe to insure it can be written to SQL.'''
    pass

# def __attempt_write(self, table_name, dataframe, cursor_method, statement, args: list = None):
#     '''Execute a statement using a pyodbc.cursor method until all built in methods to handle errors
#     have been exhausted. Raises general errors to prevent exposing injection attempts.

#     Parameters
#     ----------

#     table_name (str) : name of the SQL table
#     dataframe (pandas.DataFrame): tabular data that is being written to an SQL table
#     cursor_method (str) : 'execute'|'executemany', cursor method used to write data
#     statement (str) : statement to execute
#     args (list|None) : arguments to pass to cursor_method when executing statement, if None build from dataframe values

#     Returns
#     -------
#     None

#     '''

#     # derive args from dataframe values
#     derive = False
#     if args is None:
#         derive = True

#     for idx in range(0,self.adjust_sql_attempts,1):
#         error_class = errors.SQLGeneral("Generic SQL error in write.__attempt_write")
#         cursor = self.__connection__.connection.cursor()
#         cursor.fast_executemany = self.__connection__.fast_executemany 
#         try:
#             # prepare values for writting to SQL
#             dataframe, inputsizes = self.__prepare_write(table_name, dataframe)
#             # derive args each loop incase handling error adjusted dataframe contents
#             if derive:
#                 args = dataframe.values.tolist()
#             # set input size and data type to prevent errors when using fast_executemany=True
#             cursor.setinputsizes(inputsizes)
#             # call cursor.execute/cursor.executemany
#             if cursor_method=='execute':
#                 cursor.execute(statement, args)
#             elif cursor_method=='executemany':
#                 cursor.executemany(statement, args)
#             error_class = None
#             cursor.commit()
#             break
#         except (pyodbc.ProgrammingError, pyodbc.DataError, pyodbc.IntegrityError) as odbc_error:
#             cursor.rollback()
#             error_class, undefined_columns = self.__classify_error(table_name, dataframe, odbc_error)
#             dataframe, error_class = self.__handle_error(table_name, dataframe, error_class, undefined_columns)
#             if error_class is not None:
#                 raise error_class
#         except errors.SQLTableDoesNotExist as error_class:
#             cursor.rollback()
#             dataframe, error_class = self.__handle_error(table_name, dataframe, error_class, undefined_columns=[])
#         except errors.SQLInvalidDataType as error_class:
#             raise error_class
#         except Exception as error:
#             # unclassified error
#             raise error
#     # raise exception that can't be handled
#     if error_class is not None:
#         raise error_class
#     # max adjust_sql_attempts reached
#     if idx==self.adjust_sql_attempts-1:
#         raise RecursionError(f'adjust_sql_attempts={self.adjust_sql_attempts} reached')


# def __classify_error(self, table_name: str, dataframe: pd.DataFrame, odbc_error: pyodbc.Error):
#     '''Classify an ODBC write error so it can be handled
    
#     Parameters
#     ----------
#     table_name (str) : name of table to adjust
#     dataframe (pandas.DataFrame): tabular data that is being written to an SQL table
#     odbc_error (pyodbc.Error) : a pyodbc error

#     Returns
#     -------

#     error_class (mssql_dataframe.core.errors) : classified error
#     undefined_columns (list) : columns that are not defined in SQL table
#     '''

#     error_string = str(odbc_error)
#     undefined_columns = []
    
#     if 'Invalid object name' in error_string:
#         error_class =  errors.SQLTableDoesNotExist(f"{table_name} does not exist")
#     elif 'Invalid column name' in error_string:
#         undefined_columns = re.findall(r"Invalid column name '(.+?)'", error_string)
#         undefined_columns = list(set(undefined_columns))
#         error_class =  errors.SQLColumnDoesNotExist(f"Columns {undefined_columns} do not exist in {table_name}")
#     elif 'String data, right truncation' in error_string or 'String or binary data would be truncated' in error_string:
#         # additionally check schema for better error classification
#         try:
#             schema = helpers.get_schema(self.__connection__, table_name)
#             columns, _, _, _ = helpers.flatten_schema(schema)
#             _ = helpers.dtype_py(dataframe.select_dtypes('object'), columns)
#             error_class = errors.SQLInsufficientColumnSize(f"A string column in {table_name} has insufficient size.")
#         except errors.SQLTableDoesNotExist as error:
#             # BUG: https://github.com/mkleehammer/pyodbc/issues/940
#             error_class = error
#         except errors.SQLInvalidDataType as error:
#             # example: string data is not being written to non-string column
#             raise error
#     elif 'Numeric value out of range' in error_string or 'Arithmetic overflow error' in error_string:
#         error_class = errors.SQLInsufficientColumnSize(f"A numeric column in {table_name} has insuffcient size.")
#     elif 'Invalid character value for cast specification' in error_string or 'Restricted data type attribute violation' in error_string:
#         error_class = errors.SQLInvalidInsertFormat(f"A column in {table_name} is incorrectly formatted for insert.")
#     elif isinstance(odbc_error, pyodbc.IntegrityError):
#         # allowable visible exception for attempt to insert duplicated primary key value
#         error_class = odbc_error
#     else:
#         error_class = errors.SQLGeneral("Generic SQL error in write.__classify_error")

#     return error_class, undefined_columns


# def __handle_error(self, table_name: str, dataframe: pd.DataFrame, error_class: errors, undefined_columns: list):
#     ''' Handle an SQL write error by rasing an appropriate exeception or adjusting the SQL table. If adjust_sql_objects==True,
#     the table may be created or columns may be added or modified.

#     Parameters
#     ----------

#     table_name (str) : name of table to adjust
#     dataframe (pandas.DataFrame) : tabular data that was attempted to be written
#     error_class (mssql_dataframe.core.errors) : classified error
#     undefined_columns (list) : columns that are not defined in SQL table

#     Returns
#     -------
#     dataframe (pandas.DataFrame) : data that may have been modified if table was created
#     error_class (mssql_dataframe.core.errors|None) : None if error was handled

#     '''

#     # always add include_timestamps columns
#     include_timestamps = [x for x in undefined_columns if x in ['_time_update', '_time_insert']]
#     if include_timestamps:
#         error_class = None
#         for column in include_timestamps:
#             warnings.warn('Creating column {} in table {} with data type DATETIME2.'.format(column, table_name), errors.SQLObjectAdjustment)
#             self.__modify__.column(table_name, modify='add', column_name=column, data_type='DATETIME2')

#     # raise error since adjust_sql_objects==False
#     elif not isinstance(error_class, errors.SQLGeneral) and not self.adjust_sql_objects:
#         error_class.args = (error_class.args[0],'Initialize with parameter adjust_sql_objects=True to create/modify SQL objects.')
#         raise error_class

#     # handle error since adjust_sql_objects==True
#     else:

#         # SQLTableDoesNotExist
#         # # create table
#         if isinstance(error_class, errors.SQLTableDoesNotExist):
#             error_class = None
#             warnings.warn('Creating table {}'.format(table_name), errors.SQLObjectAdjustment)
#             dataframe = self.__create__.table_from_dataframe(table_name, dataframe, primary_key='infer')

#         # SQLColumnDoesNotExist
#         # # create missing columns
#         elif isinstance(error_class, errors.SQLColumnDoesNotExist):
#             error_class = None
#             schema = helpers.get_schema(self.__connection__, table_name)
#             table_temp = "##write_new_column_"+table_name
#             new = dataframe.columns[~dataframe.columns.isin(schema.index)]
#             dtypes_sql = helpers.infer_datatypes(self.__connection__, table_temp, dataframe[new])
#             for column, data_type in dtypes_sql.items():
#                 # warn if not a global temporary table for update/merge operations
#                 if not table_name.startswith("##__update") and not table_name.startswith("##__merge"):
#                     warnings.warn('Creating column {} in table {} with data type {}.'.format(column, table_name, data_type), errors.SQLObjectAdjustment)
#                 self.__modify__.column(table_name, modify='add', column_name=column, data_type=data_type, notnull=False)
#             # drop intermediate temp table
#             table_temp = helpers.safe_sql(self.__connection__, table_temp)
#             cursor = self.__connection__.connection.cursor()
#             cursor.execute('DROP TABLE '+table_temp)
#             cursor.commit()

#         # SQLInsufficientColumnSize
#         # # change data type and/or size (ex: tinyint to int or varchar(1) to varchar(2))
#         elif isinstance(error_class, errors.SQLInsufficientColumnSize):
#             error_class = None
#             schema = helpers.get_schema(self.__connection__, table_name)
#             table_temp = "##write_alter_column_"+table_name
#             dtypes_sql = helpers.infer_datatypes(self.__connection__, table_temp, dataframe)
#             columns, notnull, primary_key_column, _ = helpers.flatten_schema(schema)
#             adjust = {k:v for k,v in dtypes_sql.items() if v!=columns[k]}
#             for column, data_type in adjust.items():
#                 # warn if not a global temporary table for update/merge operations
#                 if not table_name.startswith("##__update") and not table_name.startswith("##__merge"):
#                     warnings.warn('Altering column {} in table {} from data type {} to {}.'.format(column, table_name, columns[column], data_type), errors.SQLObjectAdjustment)
#                 is_nullable = column in notnull
#                 if column==primary_key_column:
#                     # get primary key name
#                     primary_key_name, primary_key_column = helpers.get_pk_details(self.__connection__, table_name)
#                     # drop primary key constraint
#                     self.__modify__.primary_key(table_name, modify='drop', columns=primary_key_column, primary_key_name=primary_key_name)
#                     # alter column
#                     self.__modify__.column(table_name, modify='alter', column_name=column, data_type=data_type, notnull=True)
#                     # readd primary key constrain
#                     self.__modify__.primary_key(table_name, modify='add', columns=primary_key_column, primary_key_name=primary_key_name)
#                 else:
#                     self.__modify__.column(table_name, modify='alter', column_name=column, data_type=data_type, notnull=is_nullable)
#             # drop intermediate temp table
#             table_temp = helpers.safe_sql(self.__connection__, table_temp)
#             cursor = self.__connection__.connection.cursor()
#             cursor.execute('DROP TABLE '+table_temp)
#             cursor.commit()

    
#     return dataframe, error_class


# def __prep_update_merge(self, table_name, match_columns, dataframe, operation: Literal['update','merge']):

#     if isinstance(match_columns,str):
#         match_columns = [match_columns]

#     # read target table schema
#     schema = helpers.get_schema(self.__connection__, table_name)

#     # check validitiy of match_columns, use primary key if needed
#     if match_columns is None:
#         match_columns = list(schema[schema['is_primary_key']].index)
#         if not match_columns:
#             raise errors.SQLUndefinedPrimaryKey('SQL table {} has no primary key. Either set the primary key or specify the match_columns'.format(table_name))
#     # check match_column presence is SQL table
#     if sum(schema.index.isin(match_columns))!=len(match_columns):
#         raise errors.SQLColumnDoesNotExist('one of match_columns {} is not found in SQL table {}'.format(match_columns,table_name))
#     # check match_column presence in dataframe, use dataframe index if needed
#     if any(dataframe.index.names):
#         dataframe = dataframe.reset_index()
#     if sum(dataframe.columns.isin(match_columns))!=len(match_columns):
#         raise errors.DataframeUndefinedColumn('one of match_columns {} is not found in the input dataframe'.format(match_columns))

#     # check for new columns instead of relying on _attempt_write to prevent error for both temp table and target table
#     undefined_columns = list(dataframe.columns[~dataframe.columns.isin(schema.index)])
#     if any(undefined_columns):
#         error_class = errors.SQLColumnDoesNotExist(f'Invalid column name: {undefined_columns}')
#         dataframe,_ = self.__handle_error(table_name, dataframe, error_class, undefined_columns)
#         schema = helpers.get_schema(self.__connection__, table_name)

#     # insert data into temporary table to use for updating/merging
#     table_temp = "##__"+operation+"_"+table_name
#     temp = schema[schema.index.isin(list(dataframe.columns)+[dataframe.index.name])]
#     columns, notnull, primary_key_column, _ = helpers.flatten_schema(temp)
#     self.__create__.table(table_temp, columns, notnull, primary_key_column, sql_primary_key=False)
#     self.insert(table_temp, dataframe, include_timestamps=False)

#     return dataframe, match_columns, table_temp
