CREATE TABLE #AUTOSCHEMA (
    _VARCHAR VARCHAR(MAX),
    _TINYINT VARCHAR(MAX), _SMALLINT VARCHAR(MAX), _INT VARCHAR(MAX), _BIGINT VARCHAR(MAX),
    _NUMERIC VARCHAR(MAX), _FLOAT VARCHAR(MAX),
    _TIME VARCHAR(MAX), _DATETIME VARCHAR(MAX)
)

INSERT INTO #AUTOSCHEMA VALUES 
--VARCHAR  TINYINT  SMALLINT  INT       BIGINT        NUMERIC  FLOAT        TIME         DATETIME
('a',      '1',     '256',   '32768',  '2147483648',  '1.11',  '1.111111',  '01:00:00',  '2021-03-06 08:00:00'),
('2',      '2',     '2',     '2',      '2',           '2',     '2.222222',  '02:00:00',  '2021-03-06 08:00:00'),
('3',      '3',     '3',     '3',      '3',           '3',     '3.333333',  '03:00:00',  '2021-03-06 08:00:00'),
('4',      '4',     '4',     '4',      '4',           '4',     '4.444444',  '08:00:00',  '2021-03-06 08:00:00'),
('5',      '5',     '5',     '5',      '5',           '5',     '5.555555',  '08:00:00',  '2021-03-06 08:00:00')


SELECT ColumnName, 
(CASE 
    WHEN count(try_convert(TINYINT, _Column)) = count(_Column) THEN 'TINYINT'
    WHEN count(try_convert(SMALLINT, _Column)) = count(_Column) THEN 'SMALLINT'
    WHEN count(try_convert(INT, _Column)) = count(_Column) THEN 'INT'
    WHEN count(try_convert(BIGINT, _Column)) = count(_Column) THEN 'BIGINT'
    WHEN count(try_convert(TIME, _Column)) = count(_Column) 
        AND SUM(CASE WHEN try_convert(DATE, _Column) = '1900-01-01' THEN 0 ELSE 1 END) = 0
        THEN 'TIME'
    WHEN count(try_convert(DATETIME, _Column)) = count(_Column) THEN 'DATETIME'
    WHEN count(try_convert(FLOAT, _Column)) = count(_Column) THEN 'FLOAT'
    ELSE 'VARCHAR(255)'
END) AS column_type
FROM #AUTOSCHEMA
CROSS APPLY (VALUES
('_TINYINT', _TINYINT), 
('_SMALLINT', _SMALLINT),
('_INT', _INT),
('_BIGINT', _BIGINT),
('_INT', _INT),
('_TIME', _TIME),
('_DATETIME', _DATETIME),
('_NUMERIC', _NUMERIC),
('_FLOAT', _FLOAT),
('_VARCHAR', _VARCHAR)
) v(ColumnName, _Column)

WHERE _Column IS NOT NULL
GROUP BY ColumnName;

DROP TABLE #AUTOSCHEMA