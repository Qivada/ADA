# Modern Data Platform Initiative

Collection of policies related to modern data platform

## Deploy with PowerShell

````powershell
$policydefinitions = "https://raw.githubusercontent.com/Qivada/ADA/main/AzureDeployment/policy/modern-data-platform-initiative/azurepolicyset.json"
$policyset = New-AzPolicySetDefinition -Name "modern-data-platform-initiative" -DisplayName "Modern data platform policy initiative" -Description "Collection of policies related to modern data platform" -PolicyDefinition $policydefinitions
 
New-AzPolicyAssignment -PolicySetDefinition $policyset -Name <assignmentname> -Scope <scope>
````

## Deploy with CLI

````
az policy set-definition create --name "modern-data-platform-initiative" --display-name "Modern data platform policy initiative" --description "Collection of policies related to modern data platform" --definitions "https://raw.githubusercontent.com/Qivada/ADA/main/AzureDeployment/policy/modern-data-platform-initiative/azurepolicyset.json" 

az policy assignment create --name <assignmentName> --scope <scope> --policy-set-definition "modern-data-platform-initiative"
````
