import warnings

import pandas as pd

# from mssql_dataframe.core import errors, conversion, dynamic, infer, modify, create
from mssql_dataframe.core.write.insert import insert


class update(insert):


    def update(self, table_name: str, dataframe: pd.DataFrame, match_columns: list = None,
    include_timestamps: bool = True):
        """Update column(s) in an SQL table using a dataframe.

        Parameters
        ----------

        table_name (str) : name of table to insert data into
        dataframe (pandas.DataFrame): tabular data to insert
        match_columns (list, default=None) : matches records between dataframe and SQL table, if None the SQL primary key is used
        include_timestamps (bool, default=True) : include _time_update column which is in server time

        Returns
        -------
        
        None

        Examples
        --------

        #### update ColumnA only using the dataframe index & SQL primary key
        write.update('SomeTable', dataframe[['ColumnA']])

        #### update ColumnA and do not include a _time_update column value
        write.update('SomeTable', dataframe[['ColumnA']], include_timestamps=False)
        
        #### update Column A based on ColumnB and ColumnC, that do not have to be the SQL primary key
        write.update('SomeTable', dataframe[['ColumnA','ColumnB','ColumnC']], match_columns=['ColumnB','ColumnC'])

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
                    {update_syntax}+
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
        if include_timestamps:
            update_syntax = "'_time_update=GETDATE(),'+"+update_syntax

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

        # execute statement, and potentially handle errors
        self.__attempt_write(table_name, dataframe, 'execute', statement, args)
        cursor = self.__connection__.connection.cursor()
        table_temp = dynamic.escape(cursor, table_temp)
        cursor.execute('DROP TABLE '+table_temp)
        cursor.commit()