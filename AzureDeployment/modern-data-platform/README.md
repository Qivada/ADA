# Modern Data Platform

[![Deploy To Azure](https://raw.githubusercontent.com/Azure/azure-quickstart-templates/master/1-CONTRIBUTION-GUIDE/images/deploytoazure.svg?sanitize=true)](https://portal.azure.com/#create/Microsoft.Template/uri/https%3A%2F%2Fraw.githubusercontent.com%2FQivada%2FADA%2Fmain%2FAzureDeployment%2Fmodern-datawarehouse%2Fazuredeploy.json)

## Template Deployment
~~~mermaid
graph TB
    subgraph VNET[Azure VNET]
        SNET_PUBLIC[Databricks Public Subnet]
        SNET_PRIVATE[Databricks Private Subnet]
    end
    
    subgraph NAT[Azure NAT Gateway]
        NAT_PUBLIC_IP[Static Public IP]
    end
    
    subgraph SYNAPSE_ANALYTICS[Azure Synapse Analytics]
      SYNAPSE_WORKSPACE[Workspace]
      SYNAPSE_WORKSPACE_MANAGED_IDENTITY[Managed Identity]      
    end
    
    subgraph DATA_LAKE[Data Lake Storage]
      DATA_LAKE_STORAGE[Storage]
      DATA_LAKE_STORAGE ---|Container| DATA_LAKE_STORAGE_CONTAINER_DATAHUB[Datahub]
      DATA_LAKE_STORAGE ---|Container| DATA_LAKE_STORAGE_CONTAINER_ARCHIVE[Archive]
      DATA_LAKE_STORAGE ---|Container| DATA_LAKE_STORAGE_CONTAINER_INGEST[Ingest]
      DATA_LAKE_STORAGE ---|Container| DATA_LAKE_STORAGE_CONTAINER_SYNAPSE[Synapse]
      DATA_LAKE_STORAGE ---|Container| DATA_LAKE_STORAGE_CONTAINER_DATABRICKS[Databricks]
    end
    
    VNET --- NAT
    
    DATABRICKS[Azure Databricks]
    KEY_VAULT[Key Vault]    
    
    DATABRICKS --- SNET_PUBLIC
    DATABRICKS --- SNET_PRIVATE
    
    SYNAPSE_WORKSPACE_MANAGED_IDENTITY ---|Storage Blob Data Contributor|DATA_LAKE
    SYNAPSE_WORKSPACE_MANAGED_IDENTITY ---|Owner|DATABRICKS
    
    NAT_PUBLIC_IP ---|Firewall|KEY_VAULT    
    SYNAPSE_ANALYTICS ---|Firewall|DATA_LAKE
    SNET_PUBLIC ---|Firewall|DATA_LAKE
    SNET_PUBLIC ---|Firewall|KEY_VAULT
~~~
