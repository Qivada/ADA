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
