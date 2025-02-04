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
