# Prerequisites
1. Azure Data Lake Storage Gen2 contains following containers:
   - archive
   - datahub
2. Azure application registration (app) for authorizing Azure databricks operations against Azure Data Lake Storage Gen2 (data lake)
   - The app is authorized for data lake archive and datahub containers
   - Secret is known for the app

> **Warning**
> Container names are case sensitive

# Required Configuration
1. Attach Azure Key Vault into Azure Databricks using scope name 'KeyVault'
   - Note! Use common scope name between environments
2. Create following secrets into the attached Azure Key Vault:
   - App-databricks-id
     - Client ID for Azure application registration
   - App-databricks-secret
     - Secret for Azure application registration
   - App-databricks-tenant-id
     - Azure Active Directory tenant ID for Azure application registration
   - Storage-Name
     - Name of Azure Data Lake Storage Gen2 account

# FAQ
**Q: There are some __??? columns in data that do not exist in source. What are these columns?**
 - Columns are additional information included during data load. The columns are:
   - __HashDiff
     - Checksum calculated over all columns. Used to track data changes from the row.
   - __DeletedDatetimeUTC
     - Datetime (UTC) when row was marked deleted. Note that the datetime value is technical processing date within Databricks and not the actual datetime value when the row was deleted from source.
   - __ModifiedDatetimeUTC
     - Datetime (UTC) when row was modified. Note that the datetime value is technical processing date within Databricks and not the actual datetime value when the row was modified in source.
   - __ArchiveDatetimeUTC
     - Datetime (UTC) when row was archived into data lake.
   - __OriginalStagingFileName
     - Original file name that was archived from ingest area into archive.
 
 **Q: What will happen if source data structure and/or data types are changed over time?**
 - Data hub will automatically create new columns as required. Also data type is updated as required as long as the data type change is compatible e.g. string data type cannot be changed to double data type. Note that column(s) are not dropped from data hub table in case column(s) no longer exists in source.
