# Microsoft Fabric

## Pre-Deployment
1. Register application (service principal) for ADA work flow. See instructions [here](https://github.com/Qivada/ADA/tree/main/AzureDeployment/register-app)
2. Configure and grant admin consent to following "Power BI Service" delegated permissions for the service principal created on previous step
   - Workspace.ReadWrite.All
     - Required to check Git connection and to list lakehouses, warehouses, data pipelines and notebooks
   - Workspace.GitCommit.All
     - Required for committing changes to Git
   - Item.ReadWrite.All
     - Required to create data pipelines and notebooks
   - Item.Execute.All
     - Required to run data pipelines and notebooks

## Library
See implementation examples fron [library](https://github.com/Qivada/ADA/tree/main/AzureDeployment/microsoft-fabric/library) folder

## Notice!
As of tenant version 8.18.04 ADA no longer requires Entra user to connect with Microsoft Fabric. Work flow orchestrations are using service principal (SPN) authenthication with Microsoft Fabric starting 2025-04-11
