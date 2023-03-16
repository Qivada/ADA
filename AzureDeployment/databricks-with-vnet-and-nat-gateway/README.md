# AzureDatabricks Template with VNET and NAT Gateway

[![Deploy To Azure](https://raw.githubusercontent.com/Azure/azure-quickstart-templates/master/1-CONTRIBUTION-GUIDE/images/deploytoazure.svg?sanitize=true)](https://portal.azure.com/#create/Microsoft.Template/uri/https%3A%2F%2Fraw.githubusercontent.com%2FQivada%2FADA%2Fmain%2FAzureDeployment%2Fdatabricks-with-vnet-and-nat-gateway%2Fazuredeploy.json)

## Deployment Template Content
~~~mermaid
graph TB
    subgraph VNET[Azure VNET]
        SNET_PUBLIC[Databricks Public Subnet]
        SNET_PRIVATE[Databricks Private Subnet]
    end
    
    subgraph NAT[Azure NAT Gateway]
        NAT_PUBLIC_IP[Static Public IP]
    end
    
    SNET_PUBLIC --- NAT
    SNET_PRIVATE --- NAT
    
    DATABRICKS[Azure Databricks]
    
    DATABRICKS --- SNET_PUBLIC
    DATABRICKS --- SNET_PRIVATE   
~~~

## Required Manual Steps After Template Deployment
1. Configure Azure Key Vault connection into Databricks (if Azure Key Vault is used to store secrets)
   - Use scope name 'KeyVault' to support default configuration.
   - Configuration dialog is on location: https://**[workspace unique id]**.azuredatabricks.net/#secrets/createScope
2. Create cluster with following configuration:
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
3. If using Azure Databricks with Azure Data Factory V2 or Azure Synapse Analytics, configure related system assigned managed identity of the service as owner of the Azure Databricks workspace. This allows the service to use cluster(s) from the Azure Databricks workspace using the system assigned managed identity as authentication method.
4. Use provided Azure Databricks [notebooks templates](https://github.com/Qivada/ADA/tree/main/AzureDatabricks/__Library)
   - Make sure to place notebooks into repos of Azure Databricks workspace
   - Make sure that each Azure Databricks workspace is connected to related repository branch e.g. 'development' branch is used with development workspace and 'production' branch is used with production workspace. This will allow code deployment between Azure Databricks worspakces through repository branches.
