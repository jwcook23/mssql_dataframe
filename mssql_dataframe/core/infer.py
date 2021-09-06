'''Functions for inferring SQL properties from a dataframe containing string values.'''

from datetime import time

import pandas as pd

from mssql_dataframe.core import conversion, errors


def sql(dataframe):
    ''' Infer best fit data types from string values. 
    
    Non-object like columns will not be converted to a different pandas type.
    
    Parameters
    ----------
    dataframe (pandas.DataFrame) : object columns composed of strings and/or None

    Returns
    -------
    dataframe (pandas.DataFrame) : object columns converted to best fit pandas data type
    dtypes (dict) : derived SQL data type where key is the column name and value is the SQL type
    notnull (pandas.Index) : columns that should not be null
    pk (str) : name of column that best fits as the primary key

    '''

    # numeric like: bit, tinyint, smallint, int, bigint, float
    dataframe = convert_numeric(dataframe)

    # datetime like: time, date, datetime2
    dataframe = convert_date(dataframe)

    # string like: varchar, nvarchar
    dataframe = convert_string(dataframe)

    # determine SQL properties
    dtypes = sql_dtype(dataframe)
    notnull, pk = sql_unique(dataframe, dtypes)

    return dataframe, dtypes, notnull, pk


def convert_numeric(dataframe):
    ''' Convert objects to nullable boolean, nullable integer, or nullable float data type.
    
    Parameters
    ----------
    dataframe (pandas.DataFrame) : object columns composed of strings and/or None

    Returns
    -------
    dataframe (pandas.DataFrame) : object columns converted to nullable numeric like data type
    '''

    # convert non-nullable integers to nullable integer types
    convert = {k:v.name for k,v in dataframe.dtypes.items() if v.name.startswith('int')}
    convert = {k:v[0].upper()+v[1::] for k,v in convert.items()}
    dataframe = dataframe.astype(convert)

    # attempt conversion of object columns to numeric
    columns = dataframe.columns[dataframe.dtypes=='object']
    dataframe[columns] = dataframe[columns].replace({'True': '1', 'False': '0'})
    for col in columns:
        # skip missing since pd.to_numeric doesn't work with nullable integer types
        notna = ~dataframe[col].isna()
        if (notna==False).all():
            continue
        try:
            converted = pd.to_numeric(dataframe.loc[notna,col], downcast='integer')
            dataframe.loc[notna,col] = converted
            name = converted.dtype.name
            # convert to nullable integer type
            if converted.dtype.name.startswith('int'):
                name = name[0].upper()+name[1::]
            dataframe[col] = dataframe[col].astype(name)
        except:
            continue

    # convert Int8 to nullable boolean if only 0,1, or NA
    columns = [k for k,v in dataframe.dtypes.items() if v.name=='Int8']
    dataframe[columns] = dataframe[columns].astype('boolean', errors='ignore')

    # # convert Int16 to UInt8 (0-255 to bring inline with SQL TINYINT)
    columns = [k for k,v in dataframe.dtypes.items() if v.name=='Int16']
    # insure conversion doesn't change values outside of range to limit of 0 or 255
    converted = dataframe[columns].astype('UInt8')
    skip = ((dataframe[columns]==converted)==False).any()
    columns = [x for x in columns if x not in skip[skip].index]
    dataframe[columns] = dataframe[columns].astype('UInt8')

    return dataframe


def convert_date(dataframe):
    ''' Convert objects to nullable time delta or nullable datetime data type.

    Parameters
    ----------
    dataframe (pandas.DataFrame) : object columns composed of strings and/or None

    Returns
    -------
    dataframe (pandas.DataFrame) : object columns converted to nullable time delta or nullable datetime
    '''

    # attempt conversion of object columns to timedelta
    columns = dataframe.columns[dataframe.dtypes=='object']
    for col in columns:
        if dataframe[col].isna().all():
            continue
        dataframe[col] = pd.to_timedelta(dataframe[col], errors='ignore')
    # attempt conversion of object columns to timedelta
    columns = dataframe.columns[dataframe.dtypes=='object']
    for col in columns:
        if dataframe[col].isna().all():
            continue
        dataframe[col] = pd.to_datetime(dataframe[col], errors='ignore')

    return dataframe


def convert_string(dataframe):
    ''' Convert objects to nullable string data type.

    Parameters
    ----------
    dataframe (pandas.DataFrame) : object columns composed of strings and/or None

    Returns
    -------
    dataframe (pandas.DataFrame) : object columns converted to nullable string
    '''

    columns = dataframe.columns[dataframe.dtypes=='object']
    dataframe[columns] = dataframe[columns].astype('string')

    return dataframe


def sql_unique(dataframe, dtypes):
    ''' Determine if columns should be nullable in SQL and determine best fitting primary key column.
    
    Parameters
    ----------
    dataframe (pandas.DataFrame) : columns to check

    Returns
    -------
    notnull (pandas.Index) : columns that should not be null
    pk (str) : name of column that best fits as the primary key
    
    '''

    # determine columns not nullable
    notnull = dataframe.notna().all()
    notnull = notnull[notnull].index

    # primary key can't be null
    dataframe = dataframe[notnull]

    # primary key must be all unique values
    unique = dataframe.nunique()==len(dataframe)
    dataframe = dataframe[unique[unique].index]

    # primary key 
    dtypes = dtypes.loc[dataframe.columns]
    dtypes.index.name = 'column_name'
    dtypes = dtypes.reset_index()
    ## attempt to use smallest sized numeric value
    check = pd.Series(['tinyint','smallint','int','bigint','float'], name='sql')
    pk = pd.DataFrame(check).merge(dtypes, left_on='sql', right_on='sql')
    if len(pk)>0:
        pk = dataframe[pk['column_name']].max().idxmin()    
    else:
        pk = None
    ## attempt to use smallest size string value
    if pk is None:
        check = pd.Series(['varchar','nvarchar'], name='sql')
        pk = pd.DataFrame(check).merge(dtypes, left_on='sql', right_on='sql')
        if len(pk)>0:
            pk = dataframe[pk['column_name']].apply(lambda x: x.str.len().max()).idxmin()
        else:
            pk = None

    return notnull, pk


def sql_dtype(dataframe):
    ''' Determine SQL data type based on pandas data type.
    
    Parameters
    ----------

    dataframe (pandas.DataFrame) : data to determine SQL type for

    Returns
    -------

    dtypes (dict) : derived SQL data type where key is the column name and value is the SQL type

    '''
    
    # determine data type based on conversion rules
    dtypes = pd.DataFrame(dataframe.dtypes.copy(), columns=['pandas'])
    dtypes.index.name = 'column_name'
    dtypes = dtypes.reset_index()
    dtypes['pandas'] = dtypes['pandas'].apply(lambda x: x.name)
    dtypes = dtypes.merge(conversion.rules, left_on='pandas', right_on='pandas', how='left')
    missing = dtypes.isna().any(axis='columns')
    if any(missing):
        missing = dtypes.loc[missing, 'column_name'].to_list()
        raise errors.UndefinedConversionRule(f'columns: {missing}') 
    dtypes = dtypes.set_index(keys='column_name')

    # determine SQL type for pandas string
    dtypes = _deduplicate_string(dataframe, dtypes)

    # determine SQL type for pandas datetime64[ns]
    dtypes = _deduplicate_datetime(dataframe, dtypes)

    return dtypes


def _deduplicate_string(dataframe, dtypes):
    ''' Determine if pandas string should be SQL varchar or nvarchar.

    Parameters
    ----------
    dataframe (pandas.DataFrame) : data to resolve
    dtypes (pandas.DataFrame) : conversion information for each column

    Return
    ------
    dtypes (pandas.DataFrame) : resolved data types
    '''

    deduplicate = dtypes[dtypes['pandas']=='string']
    columns = deduplicate.index.unique()
    for col in columns:
        # if encoding removes characters or all are None then assume nvarchar
        pre = dataframe[col].str.len()
        post = dataframe[col].str.encode('ascii',errors='ignore').str.len().astype('Int64')
        if pre.ne(post).any() or dataframe[col].isna().all():
            resolved = deduplicate[deduplicate['sql']=='nvarchar'].loc[col]
        else:
            resolved = deduplicate[deduplicate['sql']=='varchar'].loc[col]
        # add resolution into dtypes
        dtypes = dtypes[dtypes.index!=col]
        dtypes = dtypes.append(resolved)

    return dtypes


def _deduplicate_datetime(dataframe, dtypes):
    ''' Determine if pandas datetime should be SQL date or datetime2.

    Parameters
    ----------
    dataframe (pandas.DataFrame) : data to resolve
    dtypes (pandas.DataFrame) : conversion information for each column

    Return
    ------
    dtypes (pandas.DataFrame) : resolved data types
    '''
    
    deduplicate = dtypes[dtypes['pandas']=='datetime64[ns]']
    columns = deduplicate.index.unique()
    for col in columns:
        # if all time components are zero then assume date
        if (dataframe[col].dt.time.fillna(time(0,0))==time(0,0)).all():
            resolved = deduplicate[deduplicate['sql']=='date'].loc[col]
        else:
            resolved = deduplicate[deduplicate['sql']=='datetime2'].loc[col]
        # add resolution into dtypes
        dtypes = dtypes[dtypes.index!=col]
        dtypes = dtypes.append(resolved)

    return dtypes