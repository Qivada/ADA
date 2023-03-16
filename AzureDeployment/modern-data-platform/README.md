# Modern Data Platform

[![Deploy To Azure](https://raw.githubusercontent.com/Azure/azure-quickstart-templates/master/1-CONTRIBUTION-GUIDE/images/deploytoazure.svg?sanitize=true)](https://portal.azure.com/#create/Microsoft.Template/uri/https%3A%2F%2Fraw.githubusercontent.com%2FQivada%2FADA%2Fmain%2FAzureDeployment%2Fmodern-data-platform%2Fazuredeploy.json)

## Pre-Deployment
1. Register application for Azure Databricks. See instructions [here](https://github.com/Qivada/ADA/tree/main/AzureDeployment/register-app)

## Post-Deployment
1. From deployment template outputs
   - With cloud shell run outputs of 'powerShell_1' and 'powerShell_2'
   - With browser open link in output 'databricks_url' and in dialog use values 'databricks_url_???'
   ![image](https://user-images.githubusercontent.com/109618668/225620899-d1ddd196-7d91-4d82-8455-95adead1a652.png)


## Template Deployment Content
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
      SYNAPSE_WORKSPACE_MANAGED_IDENTITY{{Managed Identity}}
    end
    
    subgraph DATA_LAKE[Data Lake Storage]
      DATA_LAKE_STORAGE[Storage]
      DATA_LAKE_STORAGE ---|Container| DATA_LAKE_STORAGE_CONTAINER_DATAHUB[Datahub]
      DATA_LAKE_STORAGE ---|Container| DATA_LAKE_STORAGE_CONTAINER_ARCHIVE[Archive]
      DATA_LAKE_STORAGE ---|Container| DATA_LAKE_STORAGE_CONTAINER_INGEST[Ingest]
      DATA_LAKE_STORAGE ---|Container| DATA_LAKE_STORAGE_CONTAINER_SYNAPSE[Synapse]
      DATA_LAKE_STORAGE ---|Container| DATA_LAKE_STORAGE_CONTAINER_DATABRICKS[Databricks]
    end
    
    VNET === NAT
    
    subgraph DATABRICKS[Azure Databricks]
        DATABRICKS_WORKSPACE[Workspace]
        DATABRICKS_APP_REGISTRATION{{App Registration}}
    end
    
    subgraph KEY_VAULT[Azure Key Vault]
        KEY_VAULT_SECRETS[Secrets]
        KEY_VAULT_SECRETS ---|Secret| KEY_VAULT_SECRET_001[App-databricks-id]
        KEY_VAULT_SECRETS ---|Secret| KEY_VAULT_SECRET_002[App-databricks-tenant-id]
        KEY_VAULT_SECRETS ---|Secret| KEY_VAULT_SECRET_003[App-databricks-secret]
        KEY_VAULT_SECRETS ---|Secret| KEY_VAULT_SECRET_004[Storage-Name]
    end
    
    DATABRICKS === VNET
    
    SYNAPSE_WORKSPACE_MANAGED_IDENTITY ---|Storage Blob Data Contributor|DATA_LAKE
    SYNAPSE_WORKSPACE_MANAGED_IDENTITY ---|Owner|DATABRICKS
    DATABRICKS_APP_REGISTRATION ---|Storage Blob Data Contributor|DATA_LAKE
    
    NAT_PUBLIC_IP ---|Firewall|KEY_VAULT    
    SYNAPSE_ANALYTICS ---|Firewall|DATA_LAKE
    SNET_PUBLIC ---|Firewall|DATA_LAKE
    SNET_PUBLIC ---|Firewall|KEY_VAULT
    
    DATABRICKS_WORKSPACE ---|Secret scope|KEY_VAULT
~~~
