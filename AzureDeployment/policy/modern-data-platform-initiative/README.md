# Modern Data Platform Policy Initiative

> [!IMPORTANT]
> It is highly recommend that [Microsoft Defender for Cloud](https://portal.azure.com/#blade/Microsoft_Azure_Security/SecurityMenuBlade) is configured for the subscription and workloads related to modern data platform. Azure policies alone are not sufficient to monitor, track and alert of possible configuration -or security issues.


Collection of policies related to modern data platform:
- [Preview]: Storage account public access should be disallowed
- Auditing on Synapse workspace should be enabled
- Azure Data Factory should use a Git repository for source control
- Azure Databricks Clusters should disable public IP
- Azure Databricks Workspaces should be in a virtual network
- Azure Databricks workspaces should be Premium SKU that supports features like private link, customer-managed key for encryption
- Configure your Storage account public access to be disallowed
- Geo-redundant storage should be enabled for Storage Accounts
- Secure transfer to storage accounts should be enabled
- Storage accounts should prevent shared key access
- Storage accounts should restrict network access

## Deploy with PowerShell

````powershell
$subscriptionId = Read-Host -Prompt "Enter Azure subscription ID where to deploy initiative definition"
$assignmentName = Read-Host -Prompt "Enter Azure policy assignment name"

$policyScope = "/subscriptions/$subscriptionId"
Select-AzSubscription -SubscriptionId $subscriptionId

$policydefinitions = "https://raw.githubusercontent.com/Qivada/ADA/main/AzureDeployment/policy/modern-data-platform-initiative/azurepolicyset.definitions.json"
$policyset = New-AzPolicySetDefinition -Name "modern-data-platform-initiative" -DisplayName "Modern data platform policy initiative" -Description "Collection of policies related to modern data platform" -PolicyDefinition $policydefinitions

New-AzPolicyAssignment -PolicySetDefinition $policyset -Name $assignmentName -Scope $policyScope
````
