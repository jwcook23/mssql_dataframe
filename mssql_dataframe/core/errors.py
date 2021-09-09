'''Custom Exceptions and Warnings'''

class EnvironmentODBCDriverNotFound(Exception):
    '''Exception for not automatically determining ODBC driver.'''
    pass

class UndefinedConversionRule(Exception):
    '''Exception for undefined conversion rule between pandas, ODBC, and SQL.'''

class SQLGeneral(Exception):
    '''General error to prevent exposing SQL server specific error messages.'''
    pass

class SQLInvalidInsertFormat(Exception):
    '''Exception for value in an incorrect format such as a date string of mm/dd/yyyy.'''

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

class SQLInsufficientColumnSize(Exception):
    '''Exception for insufficient column size to insert a numeric or string.'''
    pass

class SQLInvalidLengthObjectName(Exception):
    '''Exception for an SQL object name that is too long.'''
    pass

class SQLInvalidDataType(Exception):
    '''Exception for invalid SQL data type.'''

class SQLObjectAdjustment(Warning):
    '''Warning for creating of modifying an SQL object'''
    pass

class DataframeUndefinedColumn(Exception):
    '''Exception for undefined dataframe column.'''
    pass

class DataframeInvalidDataType(Exception):
    '''Exception for a dataframe column that cannot be converted to it's correct type based on the target SQL type.'''