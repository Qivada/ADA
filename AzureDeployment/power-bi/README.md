# Power BI

## Pre-Deployment
1. Register application (service principal) for ADA work flow. See instructions [here](https://github.com/Qivada/ADA/tree/main/AzureDeployment/register-app)

## Authorize Service Principal to Power BI
1. Sign in to [Power BI](https://app.powerbi.com/home?experience=power-bi) with tenant admin account
2. Navigate to "Settings" > "Governance and insights" > "[Admin portal](https://app.powerbi.com/admin-portal)"
   - From "Tenant Settings" > "Developer settings" > "Allow service principals to use Power BI APIs"
     - Enable and allow access to the registered application (service principal)
     - Notice that the application (service principal) must be included into security group beforehand if authorization is not done on organization level for all service principals
   - From "Tenant Settings" > "Admin API settings" > "Allow service principals to use read-only admin APIs"
     - Enable and allow access to the registered application (service principal)
     - Notice that the application (service principal) must be included into security group beforehand if authorization is not done on organization level for all service principals
3. Navigate to Power BI workspace
   - Authorize the registered application (service principal) with "Contributor" access. Alternatively authorize security group that contains the application with "Contributor" access.
4. The registered application (service principal) can now refresh datasets on the authorized workspace
   - Notice that authorization might take 30 minutes or more to be effective on Power BI API calls
