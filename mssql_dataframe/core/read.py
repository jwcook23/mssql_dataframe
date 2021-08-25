from typing import Literal

import pandas as pd

from mssql_dataframe.core import helpers


class read():

    def __init__(self, connection):
        '''Class for reading from SQL tables.
        
        Parameters
        ----------
        connection (mssql_dataframe.connect) : connection for executing statement
        '''

        self.__connection__ = connection


    def select(self, table_name: str, column_names: list = None, where: str = None,
    limit: int = None, order_column: str=None, order_direction: Literal[None,'ASC','DESC'] = None) -> pd.DataFrame:
        """Select data from SQL into a dataframe.

        Parameters
        ----------

        table_name (str) : name of table to select data frame
        column_names (list|str, default=None) : list of columns to select, or None to select all
        where (str, default=None) : where clause filter to apply
        limit (int, default=None) : select limited number of records only
        order_column (str, default=None) : order results by column
        order_direction (str, default=None) : order direction

        Returns
        -------

        dataframe (pandas.DataFrame): tabular data from select statement
        
        None

        Examples
        --------

        #### select entire table

        read.select('SomeTable')

        #### specific columns
        read.select('SomeTable', column_names=['ColumnA','ColumnB']

        #### specify select criteria
        read.select('SomeTable', column_names='ColumnD', where="ColumnB>4 AND ColumnC IS NOT NULL", limit=1, order_column='ColumnB', order_direction='desc')

        """

        schema = helpers.get_schema(self.__connection__, table_name)
        primary_key = list(schema[schema['is_primary_key']].index)

        # sanitize table and column names for safe sql
        table_clean = helpers.safe_sql(self.__connection__, table_name)
        if column_names is None:
            column_names = '*'
        else:
            if isinstance(column_names, str):
                column_names = [column_names]
            # always read in the primary_key
            column_names = [x for x in primary_key if x not in column_names]+column_names
            column_names = helpers.safe_sql(self.__connection__, column_names)
            column_names = "\n,".join(column_names)

        # format optional where_statement
        if where is None:
            where_statement, where_args = ("", None)
        else:
            where_statement, where_args = helpers.where_clause(self.__connection__, where)

        # format optional limit
        if limit is None:
            limit = ""
        elif not isinstance(limit,int):
            raise ValueError("limit must be an integer")
        else:
            limit = "TOP("+str(limit)+")"

        # format optional order
        options = [None,'ASC','DESC']
        if (order_column is None and order_direction is not None) or (order_column is not None and order_direction is None):
            raise ValueError("order_column and order_direction must both be specified")
        elif order_direction not in options:
            raise ValueError("order direction must be one of: "+str(options))
        elif order_column is not None:
            order = "ORDER BY "+helpers.safe_sql(self.__connection__, order_column)+" "+order_direction
        else:
            order = ""


        # select values
        statement = """
        SELECT {limit}
            {column_names}
        FROM
            {table_name}
            {where_statement}
            {order}
        """.format(limit=limit,
            column_names=column_names, 
            table_name=table_clean, 
            where_statement=where_statement, 
            order=order
        )

        # read sql query
        if where_args is None:
            dataframe = helpers.read_query(self.__connection__, statement, schema=schema)
        else:
            dataframe = helpers.read_query(self.__connection__, statement, where_args, schema=schema)

        # set dataframe index as primary key
        if len(primary_key)>0:
            dtype_pk = dataframe.dtypes[primary_key]
            dataframe = dataframe.set_index(keys=primary_key)
            # change datatype of single index primary key (multi-index can be object only)
            if len(primary_key)==1:
                # use lowercase version to represent non-nullable datatype (ex: int64 for Int64)
                dataframe.index = dataframe.index.astype(dtype_pk[0].name.lower())

        return dataframe