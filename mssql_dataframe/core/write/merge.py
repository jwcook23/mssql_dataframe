"""Methods for merging a dataframe into an SQL table."""
from typing import List, Tuple

import pandas as pd

from mssql_dataframe.core import dynamic
from mssql_dataframe.core.write.insert import insert


class merge(insert):
    """Class for merging a dataframe into an SQL table."""

    def merge(
        self,
        table_name: str,
        dataframe: pd.DataFrame,
        match_columns: List[str] = None,
        upsert: bool = False,
        delete_requires: List[str] = None,
        include_metadata_timestamps: bool = None,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Merge a dataframe into an SQL table by updating, inserting, and/or deleting rows using Transact-SQL MERGE.

        With upsert=True, an update if exists otherwise insert action is performed without deleting anything. The
        delete_requires parameter allows for incremental merging by preventing records from being deleted if at least
        one record doesn't match in the column.

        Parameters
        ----------
        table_name (str) : name of the SQL table
        dataframe (pandas.DataFrame): tabular data to merge into SQL table
        match_columns (list, default=None) : combination of columns or index to determine matches, if None the SQL primary key and dataframe index is used
        upsert (bool, default=False) : delete records if they do not match
        delete_requires (list, default=None) : column(s) that need to have a matching row for records to be deleted
        include_metadata_timestamps (bool, default=None) : override for the class initialized parameter autoadjust_sql_objects to include _time_insert and _time_update columns

        Returns
        -------
        dataframe (pandas.DataFrame) : input dataframe that may have been altered to conform to SQL

        Examples
        --------
        A sample table to merge, created from a dataframe.
        >>> df = create.table_from_dataframe(
        ...    "##ExampleMergeDF", pd.DataFrame({"State": ["A", "B"], "ColumnA": [3, 4], "ColumnB": ["a", "b"]}), primary_key="index"
        ... )

        Merge using the dataframe's index and SQL primary key. Include the column _time_insert and _time_update (automatically created).
        >>> # delete first row
        >>> df = df[df.index != 0]
        >>> # update second row
        >>> df.loc[df.index == 1, "ColumnA"] = 5
        >>> # insert new row
        >>> new = pd.DataFrame({"State": ["C"], "ColumnA": [6], "ColumnB": ["d"]}, index=pd.Index([2], name="_index"))
        >>> df = pd.concat([df, new])
        >>> # perform the merge.
        >>> df_merged = merge("##ExampleMergeDF", df, include_metadata_timestamps=True)

        Incremntally merge a dataframe. Index=2 will remain unchanged since State=C is not part of the increment.
        >>> df_increment = pd.DataFrame.from_records([
        ...    # updated record
        ...    {"State": "B", "ColumnA": 6, "ColumnB": "d"},
        ...    # new record
        ...    {"State": "D", "ColumnA": 6, "ColumnB": "d"},
        ...    ],index=pd.Index([1, 3], name="_index"),
        ... )
        >>> df_increment = merge("##ExampleMergeDF", df_increment, delete_requires=['State'], include_metadata_timestamps=True)

        Perform an UPSERT action (if exists update, otherwise insert). Records will never be deleted.
        >>> df_upsert = pd.DataFrame.from_records([
        ...    # updated record
        ...    {"State": "B", "ColumnA": 10, "ColumnB": "x"},
        ...    # new record
        ...    {"State": "E", "ColumnA": 0, "ColumnB": "y"},
        ...    ],index=pd.Index([1, 4], name="_index"),
        ... )
        >>> df_merge = merge('##ExampleMergeDF', df_upsert, upsert=True, include_metadata_timestamps=True)
        """
        # check inputs
        if delete_requires is not None and upsert:
            raise ValueError("delete_requires can only be specified if upsert==False")

        # prevent setwithcopy errors incase a subset of columns from an original dataframe are being updated
        dataframe = dataframe.copy()

        # create cursor to perform operations
        cursor = self._connection.cursor()

        # override self.include_metadata_timestamps
        if include_metadata_timestamps is None:
            include_metadata_timestamps = self.include_metadata_timestamps

        # get target table schema, while checking for errors and adjusting data for inserting
        if include_metadata_timestamps:
            additional_columns = ["_time_update", "_time_insert"]
        else:
            additional_columns = None
        schema, dataframe, match_columns, temp_name = self._source_table(
            table_name, dataframe, cursor, match_columns, additional_columns
        )

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
            +' WHEN MATCHED THEN UPDATE SET '+{update_syntax}
            +' WHEN NOT MATCHED THEN INSERT ('+{insert_syntax}+')'
            +' VALUES ('+{insert_values}+')'
            +{delete_syntax}+';'

            EXEC sp_executesql
                @SQLStatement,
                N'@TableName SYSNAME, @TableTemp SYSNAME, {parameters}',
                @TableName=@TableName, @TableTemp=@TableTemp, {values};
        """

        # if matched, update all columns in dataframe besides match_columns
        update_columns = list(dataframe.columns[~dataframe.columns.isin(match_columns)])

        # if not matched, insert all columns in dataframe
        if any(dataframe.index.names):
            insert_columns = list(dataframe.index.names) + list(dataframe.columns)
        else:
            insert_columns = list(dataframe.columns)

        # alias columns to prevent direct input into SQL string
        alias_match = [str(x) for x in list(range(0, len(match_columns)))]
        alias_update = [str(x) for x in list(range(0, len(update_columns)))]
        alias_insert = [str(x) for x in list(range(0, len(insert_columns)))]
        if delete_requires is None:
            alias_conditions = []
        else:
            alias_conditions = [str(x) for x in list(range(0, len(delete_requires)))]

        # declare SQL variables
        declare = ["DECLARE @Match_" + x + " SYSNAME = ?;" for x in alias_match]
        declare += ["DECLARE @Update_" + x + " SYSNAME = ?;" for x in alias_update]
        declare += ["DECLARE @Insert_" + x + " SYSNAME = ?;" for x in alias_insert]
        declare += ["DECLARE @Subset_" + x + " SYSNAME = ?;" for x in alias_conditions]
        declare = "\n".join(declare)

        # form match on syntax
        match_syntax = ["QUOTENAME(@Match_" + x + ")" for x in alias_match]
        match_syntax = "+' AND '+".join(
            ["'_target.'+" + x + "+'=_source.'+" + x for x in match_syntax]
        )

        # form when matched then update syntax
        update_syntax = ["QUOTENAME(@Update_" + x + ")" for x in alias_update]
        update_syntax = "+','+".join([x + "+'=_source.'+" + x for x in update_syntax])
        if include_metadata_timestamps:
            update_syntax = "+'_time_update=GETDATE(), '+" + update_syntax

        # form when not matched then insert
        insert_syntax = "+','+".join(
            ["QUOTENAME(@Insert_" + x + ")" for x in alias_insert]
        )
        insert_values = "+','+".join(
            ["'_source.'+QUOTENAME(@Insert_" + x + ")" for x in alias_insert]
        )
        if include_metadata_timestamps:
            insert_syntax = "+'_time_insert, '+" + insert_syntax
            insert_values = "+'GETDATE(), '+" + insert_values

        # form when not matched by source then delete condition syntax
        if not upsert:
            delete_syntax = (
                "' WHEN NOT MATCHED BY SOURCE '+{conditions_syntax}+' THEN DELETE'"
            )
            # ignore hardcoded_sql_expressions since alias_conditions is used to pass delete_requires to args
            conditions_syntax = [
                "'AND _target.'+QUOTENAME(@Subset_"  # nosec hardcoded_sql_expressions
                + x
                + ")+' IN (SELECT '+QUOTENAME(@Subset_"  # nosec hardcoded_sql_expressions
                + x
                + ")+' FROM '+QUOTENAME(@TableTemp)+')'"
                for x in alias_conditions
            ]
            conditions_syntax = " + ".join(conditions_syntax)
            delete_syntax = delete_syntax.format(conditions_syntax=conditions_syntax)
        else:
            delete_syntax = "''"

        # parameters for sp_executesql
        parameters = ["@Match_" + x + " SYSNAME" for x in alias_match]
        parameters += ["@Update_" + x + " SYSNAME" for x in alias_update]
        parameters += ["@Insert_" + x + " SYSNAME" for x in alias_insert]
        parameters += ["@Subset_" + x + " SYSNAME" for x in alias_conditions]
        parameters = ", ".join(parameters)

        # values for sp_executesql
        values = ["@Match_" + x + "=@Match_" + x for x in alias_match]
        values += ["@Update_" + x + "=@Update_" + x for x in alias_update]
        values += ["@Insert_" + x + "=@Insert_" + x for x in alias_insert]
        values += ["@Subset_" + x + "=@Subset_" + x for x in alias_conditions]
        values = ", ".join(values)

        # set final SQL string
        statement = statement.format(
            declare=declare,
            match_syntax=match_syntax,
            update_syntax=update_syntax,
            insert_syntax=insert_syntax,
            insert_values=insert_values,
            delete_syntax=delete_syntax,
            parameters=parameters,
            values=values,
        )

        # perform merge
        if delete_requires is None:
            args = (
                [table_name, temp_name]
                + match_columns
                + update_columns
                + insert_columns
            )
        else:
            args = (
                [table_name, temp_name]
                + match_columns
                + update_columns
                + insert_columns
                + delete_requires
            )

        # execute statement to perform update in target table using source
        cursor.execute(statement, args)
        temp_name = dynamic.escape(cursor, temp_name)
        cursor.execute("DROP TABLE " + temp_name)
        cursor.commit()

        return dataframe
