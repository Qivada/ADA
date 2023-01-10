# Prerequisites
1. Azure Data Lake Storage Gen2 contains following containers:
   - ingest
   - archive
2. Azure application registration (app) for authorizing Azure databricks operations against Azure Data Lake Storage Gen2 (data lake)
   - The app is authorized for data lake ingest and archive containers
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
   - blob-account
     - Name of Azure Blob storage account
     - Note! Required only, if archive operation is done from Azure Blob Storage
   - blob-account-key
     - Access key (key 1 or key 2) of Azure Blob storage account
     - Note! Required only, if archive operation is done from Azure Blob Storage

# FAQ
**Q: What is purpose of log?**
 - Contains information from archived file such as archive date, original file name, size of the file etc. This log is provided so that archived files can be queried effectively from archive without need for scanning all files from archive structure. Basically this is an index for archive about information what is actually archived and to what location.
 
**Q: Why file is renamed to archive?**
 - Renaming is done to prevent collisions with existing archived files. Original file name for archived file can be queried from archive log.
