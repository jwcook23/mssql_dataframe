-- SET @sql = N' CREATE TABLE ' + QUOTENAME(@_table) + N'(ID INT);';
-- EXEC sp_executesql @sql, N'@_table sysname', @_table = @_table;


-- DECLARE @sql AS NVARCHAR(MAX);
-- DECLARE @_table sysname = '##_testing';

-- SET @sql = N' DROP TABLE ' + QUOTENAME(@_table) + N';';
-- EXEC sp_executesql @sql, N'@_table sysname', @_table = @_table;

DROP TABLE ##_testing

DECLARE @sql AS NVARCHAR(MAX);
DECLARE @_table sysname = '##_testing';
DECLARE @_column sysname = 'ID'
DECLARE @_type sysname = 'NVARCHAR';

DECLARE @_columns sysname = 'ID';
SET @sql = N' CREATE TABLE ' + QUOTENAME(@_table)+'('+
QUOTENAME(@_columns)+' '+
QUOTENAME(@_type)+
');';
EXEC sp_executesql @sql;