DROP TABLE _test

CREATE TABLE _test (
        _default VARCHAR(MAX),
        _tinyint VARCHAR(MAX),
        _int VARCHAR(MAX),
        _bigint VARCHAR(MAX),
        _date VARCHAR(MAX),
        _time VARCHAR(MAX),
        _datetime VARCHAR(MAX),
        _numeric VARCHAR(MAX),
        _float VARCHAR(MAX)
)

INSERT INTO _test
VALUES
('name1','1','1','1','11/01/2001','01:00:00','11/01/2001 01:00:00','1','1'),
('name2','2','2','2','2001-11-01','00:01:00','2001-11-01 00:01:00','2','2'),
('name3','3','3','3','11-01-2001','00:00:01','11-01-2001 00:00:01','3','3'),
('name4','4','400','2147483648','11/01/2001','01:00:00','11/01/2001 01:00:00','4.1','4.11111111111111111111')

-- SELECT *
-- FROM _test


-- SELECT COLUMN_NAME
-- FROM INFORMATION_SCHEMA.COLUMNS
-- WHERE TABLE_NAME = '_test'

SELECT column_name,
       (CASE 
            WHEN count(try_convert(TINYINT, col)) = count(col) THEN 'TINYINT'
            WHEN count(try_convert(INT, col)) = count(col) THEN 'INT'
            WHEN count(try_convert(BIGINT, col)) = count(col) THEN 'BIGINT'
            WHEN count(try_convert(DATE, col)) = count(col) THEN 'DATE'
            WHEN count(try_convert(TIME, col)) = count(col) THEN 'TIME'
            WHEN count(try_convert(DATETIME, col)) = count(col) THEN 'DATETIME'
            WHEN count(try_convert(NUMERIC(20, 4), col)) = count(col) 
                AND sum(CASE WHEN col LIKE '%._____' THEN 1 ELSE 0 END) = 0
                THEN 'numeric(20, 4)'
            WHEN count(try_convert(FLOAT, col)) = count(col) THEN 'FLOAT'
            ELSE 'VARCHAR(255)'
        END) AS column_type
FROM _test CROSS APPLY
     (VALUES ('_tinyint', _tinyint)) v(column_name, col)
WHERE col IS NOT NULL
GROUP BY column_name;