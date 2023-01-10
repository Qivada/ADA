# ADA Azure Databricks Notebooks
Collection of Azure Databricks notebooks to work with data lake and Azure Synapse Analytics.

# Data Lake Deployment Recommendations
Deploy Azure Data Lake Storage Gen2 with following containers:
- ingest
  - Landing area for incoming data
- archive
  - Files from landing area are archived into this container along with archival log information
- datahub
  - Processed data from archive e.g. change data capture (CDC) handled records over all archive files
- databricks
  - Contains temporary files etc. that are required in some cases to e.g. transfer data into data warehouse using Databricks
