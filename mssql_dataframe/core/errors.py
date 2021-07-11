'''Custom Exceptions and Warnings'''

class EnvironmentODBCDriverNotFound(Exception):
    '''Exception for not automatically determining ODBC driver.'''
    pass

class SQLGeneral(Exception):
    '''General error to prevent exposing SQL server specific error messages.'''
    pass

class SQLTableDoesNotExist(Exception):
    '''Exception for SQL table that does not exist.'''
    pass

class SQLColumnDoesNotExist(Exception):
    '''Exception for SQL table column that does not exist.'''
    pass

class SQLInvalidSyntax(Exception):
    '''Exception for invalid syntax'''
    pass

class SQLUndefinedPrimaryKey(Exception):
    '''Exception for undefined SQL primary key in table.'''
    pass

class SQLInsufficientStringColumnSize(Exception):
    '''Exception for insufficient column size to insert a string like value.'''
    pass

class SQLInsufficientNumericColumnSize(Exception):
    '''Exception for insufficient column size to insert a numeric like value.'''
    pass

class SQLUndefinedColumn(Exception):
    '''Exception for undefined SQL column.'''
    pass

class SQLInvalidLengthObjectName(Exception):
    '''Exception for an SQL object name that is too long.'''
    pass

class SQLObjectAdjustment(Warning):
    '''Warning for creating of modifying an SQL object'''
    pass

class DataframeUndefinedColumn(Exception):
    '''Exception for undefined dataframe column.'''
    pass

class DataframeUndefinedBestType(Warning):
    '''Warning for undefined best data type of dataframe column given an SQL data type.'''