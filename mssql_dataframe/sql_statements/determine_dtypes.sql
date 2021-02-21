SELECT ColumnName,
       (CASE 
            WHEN count(try_convert(TINYINT, _Column)) = count(_Column) THEN 'TINYINT'
            WHEN count(try_convert(INT, _Column)) = count(_Column) THEN 'INT'
            WHEN count(try_convert(BIGINT, _Column)) = count(_Column) THEN 'BIGINT'
            WHEN count(try_convert(DATE, _Column)) = count(_Column) THEN 'DATE'
            WHEN count(try_convert(TIME, _Column)) = count(_Column) THEN 'TIME'
            WHEN count(try_convert(DATETIME, _Column)) = count(_Column) THEN 'DATETIME'
            WHEN count(try_convert(NUMERIC(20, 4), _Column)) = count(_Column) 
                AND sum(CASE WHEN _Column LIKE '%._____' THEN 1 ELSE 0 END) = 0
                THEN 'numeric(20, 4)'
            WHEN count(try_convert(FLOAT, _Column)) = count(_Column) THEN 'FLOAT'
            ELSE 'VARCHAR(255)'
        END) AS column_type
FROM ##dtype_##test_from_dataframe_namedpk CROSS APPLY
     (VALUES ('_tinyint', _tinyint),('_bitint', _bigint)) v(ColumnName, _Column)
WHERE _Column IS NOT NULL
GROUP BY ColumnName;

-- SELECT ColumnName,
--        (CASE 
--             WHEN count(try_convert(TINYINT, _Column)) = count(_Column) THEN 'TINYINT'
--             WHEN count(try_convert(INT, _Column)) = count(_Column) THEN 'INT'
--             WHEN count(try_convert(BIGINT, _Column)) = count(_Column) THEN 'BIGINT'
--             WHEN count(try_convert(DATE, _Column)) = count(_Column) THEN 'DATE'
--             WHEN count(try_convert(TIME, _Column)) = count(_Column) THEN 'TIME'
--             WHEN count(try_convert(DATETIME, _Column)) = count(_Column) THEN 'DATETIME'
--             WHEN count(try_convert(NUMERIC(20, 4), _Column)) = count(_Column) 
--                 AND sum(CASE WHEN _Column LIKE '%._____' THEN 1 ELSE 0 END) = 0
--                 THEN 'numeric(20, 4)'
--             WHEN count(try_convert(FLOAT, _Column)) = count(_Column) THEN 'FLOAT'
--             ELSE 'VARCHAR(255)'
--         END) AS column_type
-- FROM @TableName CROSS APPLY
--      (VALUES ('_tinyint', @ColumnName_x)) v(ColumnName_x, _Column)
-- WHERE _Column IS NOT NULL
-- GROUP BY @ColumnName;
