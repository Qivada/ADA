CREATE TABLE [Configuration_Silver].[name-of-source-system].[silver]
(
	[IsActive] [bit]  NULL,
	[Type] [varchar](250)  NULL,
	[BronzeSchema] [varchar](250)  NULL,
	[BronzeTable] [varchar](250)  NULL,
	[SilverSchema] [varchar](250)  NULL,
	[SilverTable] [varchar](250)  NULL,
	[BusinessKeyColumns] [varchar](8000)  NULL,
	[ExtractColumns] [varchar](8000)  NULL,
	[ExcludeColumns] [varchar](8000)  NULL,
	[DeleteFilterColumns] [varchar](250)  NULL,
	[Tag] [varchar](8000)  NULL
)

/*
-- Data pipeline lookup usage example
SELECT DISTINCT 
       ISNULL(T1.[Type], '') AS [Type],
       ISNULL(T1.[BronzeSchema], '') AS [BronzeSchema],
       ISNULL(T1.[BronzeTable], '') AS [BronzeTable],
       ISNULL(T1.[SilverSchema], '') AS [SilverSchema],
       ISNULL(T1.[SilverTable], '') AS [SilverTable],    
       ISNULL(T1.[BusinessKeyColumns], '') AS [BusinessKeyColumns],
       ISNULL(T1.[ExtractColumns], '') AS [ExtractColumns],
       ISNULL(T1.[ExcludeColumns], '') AS [ExcludeColumns],
       ISNULL(T1.[DeleteFilterColumns], '') AS [DeleteFilterColumns]
FROM   [adventureWorksLT].[silver] AS T1
       CROSS APPLY STRING_SPLIT(T1.Tag, ',') AS T2
WHERE  T1.[IsActive] = 1 AND
        (
            LOWER(TRIM(T2.[value])) IN (
                SELECT  LOWER(TRIM(value)) AS Tag
                FROM    STRING_SPLIT('@{pipeline().parameters.Tag}', ',')
            ) OR
            TRIM('@{pipeline().parameters.Tag}') = '' OR
            TRIM('@{pipeline().parameters.Tag}') = '*'
        )
*/
