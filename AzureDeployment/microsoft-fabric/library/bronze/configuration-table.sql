CREATE TABLE [Configuration_Bronze].[name-of-source-system].[bronze]
(
	[IsActive] [bit]  NULL,
	[Query] [varchar](8000)  NULL,
	[Dataset] [varchar](250)  NULL,
	[Tag] [varchar](250)  NULL
)

/*
-- Data pipeline lookup usage example
SELECT  DISTINCT 
        T1.[Query], 
        LOWER(T1.[Dataset]) AS [Dataset]
FROM    [adventureWorksLT].[bronze] AS T1
        CROSS APPLY STRING_SPLIT(T1.Tag, ',') AS T2
WHERE   T1.[IsActive] = 1 AND
        (
            LOWER(TRIM(T2.[value])) IN (
                SELECT  LOWER(TRIM(value)) AS Tag
                FROM    STRING_SPLIT('@{pipeline().parameters.Tag}', ',')
            ) OR
            TRIM('@{pipeline().parameters.Tag}') = '' OR
            TRIM('@{pipeline().parameters.Tag}') = '*'
        )
*/
