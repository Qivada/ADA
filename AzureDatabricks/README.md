# ADA Azure Databricks Notebooks
Collection of Azure Databricks notebooks to work with data lake and Azure Synapse Analytics.

# Data Lake Deployment Recommendations
Deploy Azure Data Lake Storage Gen2 with following containers:
- ingest
  - Landing area for incoming data
- archive
  - Files from landing area are archived into this container along with archival log information
  - [Related notebooks](https://github.com/Qivada/ADA/tree/main/AzureDatabricks/__Library/FromIngestToArchive)
- datahub
  - Processed data from archive e.g. change data capture (CDC) handled records over all archive files
  - [Related notebooks](https://github.com/Qivada/ADA/tree/main/AzureDatabricks/__Library/FromArchiveToDatabricks)
- databricks
  - Contains temporary files etc. that are required in some cases to e.g. transfer data into data warehouse using Databricks

> **Warning**
> Container names are case sensitive

# Recommended Structure for Data Lake
- ingest
  - Data source e.g. AX, CRM, ...
    - Dataset e.g. LEDGERTRANS, Account, ...
- archive
  - Data source e.g. AX, CRM, ...
    - Dataset e.g. LEDGERTRANS, Account, ...
      - log
      - year e.g. 2022
        - month e.g. 12
          - day e.g. 31
- datahub
  - Data source e.g. AX, CRM, ...
    - Dataset e.g. LEDGERTRANS, Account, ...
      - log
      - data

> **Warning**
> Set access rights on root folder(s) before creating child folders.

# Recommended Data Flow for Data Lake
~~~mermaid
sequenceDiagram
  [Integrator]-->>Ingest: Send file
  loop Archive
  Archive->>Ingest: List files
  Ingest-->>Archive: Copy files
  Archive->>Archive: Create log entries
  Archive->>Ingest: Remove archived files
  end
  loop Data Hub
  Data Hub->>Data Hub: Check log
  Data Hub->>Archive: Check new files by log entry
  Archive-->>Data Hub: Process new files
  Data Hub->>Data Hub: Transform files into data hub
  Data Hub->>Data Hub: Create log entry
  end
~~~

> :information_source:
Data is commonly sent or made available to target system(s) from:
> - Archive: When original data is required as is
> - Data Hub: When only changes are required

# Recommended Azure Databricks Deployment
1. Deploy one Azure Databricks workspace per environment e.g. development, test or production.
2. Configure with 'Bring Your Own VNET'. This enables firewall configurable connections from -and to Databricks
> **Warning**
> Deployment with 'Bring Your Own VNET' is requirement for Azure Databricks to connect firewalled storage account.
3. Deploy and configure Azure Key Vault per Databricks instance to store secrets and configuration values. Use scope name 'KeyVault' to support default configuration.
   - To configure Azure Key Vault open configuration dialog from: https://**[workspace unique id]**.azuredatabricks.net/#secrets/createScope
4. Create cluster with following configuration:
   - Policy: Unrestricted
   - Access mode: No isolation shared
   - Databricks runtime version: 11.3 LTS or later LTS version
   - Use Photon accelaration: Selected
   - Worker type: Standard_D4s_v5
     - Min workers 1 and max workers e.g. 3 based on actual requirement
     - Consider using spot instances on development and test environments
   - Enable autoscaling: Selected
     - For small development and test environments autoscaling might not be required
   - Terminate after: Configure 30 minutes or less
5. If using Azure Databricks with Azure Data Factory V2 or Azure Synapse Analytics, configure related system assigned managed identity of the service as owner of the Azure Databricks workspace. This allows the service to use cluster(s) from the Azure Databricks workspace. 
