# Modern Data Platform Initiative

Collection of policies related to modern data platform

## Deploy with PowerShell

````powershell
$subscriptionId = "<Enter Azure Subscription ID>"
Select-AzSubscription -SubscriptionId $subscriptionId

$policydefinitions = "https://raw.githubusercontent.com/Qivada/ADA/main/AzureDeployment/policy/modern-data-platform-initiative/azurepolicyset.definitions.json"
New-AzPolicySetDefinition -Name "modern-data-platform-initiative" -DisplayName "Modern data platform policy initiative" -Description "Collection of policies related to modern data platform" -PolicyDefinition $policydefinitions
````
