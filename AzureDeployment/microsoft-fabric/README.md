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

## Authorize Microsoft Entra user to Microsoft Fabric
1. Create new Microsoft Entra user for Qivada ADA to use for authentication
2. Sign in to [Microsoft Fabric](https://app.fabric.microsoft.com/) and authorize the Microsoft Entra user to required workspace(s)
4. Sign in to [Azure Portal](https://portal.azure.com/) and navigate to Microsoft Entra > Security > Named locations and include Qivada ADA static public IP address 20.229.33.209 to the the named locations
   - This allows Qivada ADA to programmatically sign in to Microsoft Fabric with the Microsoft Entra user without MFA verification
