// =========================================== 
// Metadata
// ===========================================
metadata author = 'Qivada'
metadata changeHistory = [
    {
        date: '2023-03-09'
        change: 'Initial version'
    }
]

// =========================================== 
// Parameters
// ===========================================
@description('Location for all resources.')
param location string = resourceGroup().location

@description('Application (client) ID of application created for databricks')
@minLength(36)
@maxLength(36)
param databricksAppClientId string = 'Application (client) ID'

@description('Object ID of managed application created for databricks')
@minLength(36)
@maxLength(36)
param databricksManagedAppObjectId string = 'Object ID from managed app (Enterprise Application)'

@description('Secret of application created for databricks')
@minLength(40)
@maxLength(40)
@secure()
param databricksAppSecret string = ''

@description('''
Expiration date of secret of application created for databricks:
- Use format yyyy-MM-dd e.g. 2022-12-31
''')
@minLength(10)
@maxLength(10)
#disable-next-line secure-secrets-in-params
param databricksAppSecretExpire string = 'yyyy-MM-dd'

@description('Name of the NAT gateway to be attached to the workspace subnets.')
param databricksNatGatewayName string = 'nat-PROJECT-dbw-ENVIRONMENT-REGION-INSTANCE'

@description('Name of the NAT gateway public IP.')
param databricksNatGatewayPublicIpName string = 'pip-PROJECT-dbw-ENVIRONMENT-REGION-INSTANCE'

@description('The name of the network security group to create.')
param databricksNsgName string = 'nsg-PROJECT-dbw-ENVIRONMENT-REGION-INSTANCE'

@description('The name of the virtual network to create.')
param databricksVnetName string = 'vnet-PROJECT-ENVIRONMENT-REGION-INSTANCE'

@description('Cidr range for the vnet.')
param databricksVnetCidr string = '10.1.0.0/21'

@description('The name of the public subnet to create.')
param databricksPublicSubnetName string = 'snet-PROJECT-dbw-public-ENVIRONMENT-REGION-INSTANCE'

@description('Cidr range for the public subnet.')
param databricksPublicSubnetCidr string = '10.1.0.0/24'

@description('The name of the private subnet to create.')
param databricksPrivateSubnetName string = 'snet-PROJECT-dbw-private-ENVIRONMENT-REGION-INSTANCE'

@description('Cidr range for the private subnet.')
param databricksPrivateSubnetCidr string = '10.1.1.0/24'

@description('The name of the Azure Databricks workspace to create.')
param databricksWorkspaceName string = 'dbw-PROJECT-ENVIRONMENT-REGION-INSTANCE'

@description('The name of the Azure Key Vault to create.')
@minLength(3)
@maxLength(24)
param keyVaultName string = 'kv-PROJECT-ENVIRONMENT-REGION-INSTANCE'

@description('The name of the Azure data lake storage to create.')
@minLength(3)
@maxLength(24)
param dataLakeStorageName string = 'dls-PROJECT-ENVIRONMENT-REGION-INSTANCE'

@description('The name of the Azure synapse workspace to create.')
@minLength(5)
@maxLength(50)
param synapseWorkspaceName string = 'syn-PROJECT-ENVIRONMENT-REGION-INSTANCE'

@description('The name of the Azure synapse workspace SQL administrator login to create.')
@minLength(5)
@maxLength(50)
param synapseWorkspaceSqlAdministratorLogin string = ''

@description('The name of the Azure synapse workspace SQL administrator login to create.')
@minLength(10)
@maxLength(50)
@secure()
param synapseWorkspaceSqlAdministratorLoginPassword string = ''

// =========================================== 
// Variables
// ===========================================
var var_managedResourceGroupName = 'databricks-rg-${databricksWorkspaceName}-${uniqueString(databricksWorkspaceName, resourceGroup().id)}'
var var_trimmedMRGName = substring(var_managedResourceGroupName, 0, min(length(var_managedResourceGroupName), 90))
var var_managedResourceGroupId = '${subscription().id}/resourceGroups/${var_trimmedMRGName}'
var var_databricksNsgId = resourceId('Microsoft.Network/networkSecurityGroups/', databricksNsgName)
var var_databricksVnetId = resourceId('Microsoft.Network/virtualNetworks/', databricksVnetName)
var var_databricksNatId = resourceId('Microsoft.Network/natGateways/', databricksNatGatewayName)
var var_databricksNatPipId = resourceId('Microsoft.Network/publicIPAddresses/', databricksNatGatewayPublicIpName)

// =========================================== 
// Resources
// ===========================================
resource resource_databricks_nat_pip 'Microsoft.Network/publicIPAddresses@2022-09-01' = {
    location: location
    name: databricksNatGatewayPublicIpName
    sku: {
	    name: 'Standard'
    }
    properties: {
	    publicIPAddressVersion: 'IPv4'
        publicIPAllocationMethod: 'Static'
        idleTimeoutInMinutes: 4
    }
}

resource resource_databricks_nat 'Microsoft.Network/natGateways@2022-09-01' = {
    location: location
    name: databricksNatGatewayName
    sku: {
        name: 'Standard'
    }
    properties: {
        idleTimeoutInMinutes: 4
        publicIpAddresses: [
            {
                id: resourceId('Microsoft.Network/publicIPAddresses/', databricksNatGatewayPublicIpName)
            }
        ]
    }
    dependsOn: [
        resource_databricks_nat_pip
    ]
}

resource resource_databricks_nsg 'Microsoft.Network/networkSecurityGroups@2022-09-01' = {
    location: location
    name: databricksNsgName
}

resource resource_databricks_vnet 'Microsoft.Network/virtualNetworks@2022-09-01' = {
    location: location
    name: databricksVnetName
    properties: {
        addressSpace: {
            addressPrefixes: [
                databricksVnetCidr
            ]
        }
        subnets: [
            {
                name: databricksPublicSubnetName
                properties: {
                    addressPrefix: databricksPublicSubnetCidr
                    networkSecurityGroup: {
                        id: var_databricksNsgId
                    }
                    natGateway: {
                        id: var_databricksNatId
                    }
                    serviceEndpoints: [
                        {
                            service: 'Microsoft.KeyVault'
                            locations: [
                                '*'
                            ]
                        }
                        {
                            service: 'Microsoft.Storage'
                            locations: [
                                '*'
                            ]
                        }
                    ]
                    delegations: [
                        {
                            name: 'databricks-del-public'
                            properties: {
                                serviceName: 'Microsoft.Databricks/workspaces'
                            }
                        }
                    ]
                }
            }
            {
                name: databricksPrivateSubnetName
                properties: {
                    addressPrefix: databricksPrivateSubnetCidr
                    networkSecurityGroup: {
                        id: var_databricksNsgId
                    }
                    natGateway: {
                        id: var_databricksNatId
                    }
                    delegations: [
                        {
                            name: 'databricks-del-private'
                            properties: {
                                serviceName: 'Microsoft.Databricks/workspaces'
                            }
                        }
                    ]
                }
            }
        ]
    }
    dependsOn: [
        resource_databricks_nat
        resource_databricks_nsg
    ]
}

resource resource_databricks 'Microsoft.Databricks/workspaces@2023-02-01' = {
    location: location
    name: databricksWorkspaceName
    sku: {
        name: 'Premium'
    }
    properties: {
        #disable-next-line use-resource-id-functions
        managedResourceGroupId: var_managedResourceGroupId
        parameters: {
            customVirtualNetworkId: {
                value: var_databricksVnetId
            }
            customPublicSubnetName: {
                value: databricksPublicSubnetName
            }
            customPrivateSubnetName: {
                value: databricksPrivateSubnetName
            }
            enableNoPublicIp: {
                value: true
            }
        }
    }
    dependsOn: [
        resource_databricks_nsg
        resource_databricks_vnet
    ]
}

resource resource_keyvault 'Microsoft.KeyVault/vaults@2022-11-01' = {
    location: location
    name: keyVaultName
    properties: {
        enabledForDeployment: false
        enabledForDiskEncryption: false
        enabledForTemplateDeployment: false
        enableSoftDelete: true
        softDeleteRetentionInDays: 90
        enablePurgeProtection: true
        enableRbacAuthorization: false
        tenantId: subscription().tenantId
        sku: {
            family: 'A'
            name: 'standard'
        }
        accessPolicies: []
        networkAcls: {
            defaultAction: 'Deny'
            bypass: 'AzureServices'
            ipRules: [
                {
                    value: reference(var_databricksNatPipId).ipAddress
                }
            ]
            virtualNetworkRules: [
                {
                    #disable-next-line use-resource-id-functions
                    id: '${var_databricksVnetId}/subnets/${databricksPublicSubnetName}'
                    ignoreMissingVnetServiceEndpoint: false
                }
            ]
        }
        publicNetworkAccess: 'Enabled'
    }
    dependsOn: [
        resource_databricks_vnet
        resource_databricks_nat_pip
    ]

    resource resource_keyvault_secrets_app_databricks_id 'secrets' = {
        name: 'App-databricks-id'
        properties: {
            attributes: {
              enabled: true
            }
            contentType: 'string'
            value: databricksAppClientId
        }
    }

    resource resource_keyvault_secrets_app_databricks_tenant_id 'secrets' = {
        name: 'App-databricks-tenant-id'
        properties: {
            attributes: {
              enabled: true
            }
            contentType: 'string'
            value: subscription().tenantId
        }
    }

    resource resource_keyvault_secrets_app_databricks_secret 'secrets' = {
        name: 'App-databricks-secret'
        properties: {
            attributes: {
              enabled: true
              exp: dateTimeToEpoch(databricksAppSecretExpire)
            }
            contentType: 'string'
            value: databricksAppSecret
          }
    }

    resource resource_keyvault_secrets_storage_name 'secrets' = {
        name: 'Storage-Name'
        properties: {
            attributes: {
              enabled: true
            }
            contentType: 'string'
            value: resource_data_lake_storage.name
          }
    }
}

resource resource_data_lake_storage 'Microsoft.Storage/storageAccounts@2022-09-01' = {
    location: location
    name: dataLakeStorageName
    sku: {
	    name: 'Standard_GRS'
    }
    kind: 'StorageV2'
    properties: {
        dnsEndpointType: 'Standard'
        defaultToOAuthAuthentication: true
        publicNetworkAccess: 'Enabled'
        allowCrossTenantReplication: false
        isSftpEnabled: false
        minimumTlsVersion: 'TLS1_2'
        allowBlobPublicAccess: false
        allowSharedKeyAccess: false
        isHnsEnabled: true
        supportsHttpsTrafficOnly: true
        accessTier: 'Hot'
        encryption: {
            requireInfrastructureEncryption: false
            keySource: 'Microsoft.Storage'
            services: {
                file: {
                    keyType: 'Account'
                    enabled: true
                }
                blob: {
                    keyType: 'Account'
                    enabled: true
                }
	        }
        }
        networkAcls: {
            defaultAction: 'Deny'
            bypass: 'AzureServices'
            virtualNetworkRules: [
                {
                    #disable-next-line use-resource-id-functions
                    id: '${var_databricksVnetId}/subnets/${databricksPublicSubnetName}'
                    action: 'Allow'
                }
            ]
        }
    }
    dependsOn: [
        resource_databricks_vnet
        resource_databricks_nat_pip
    ]

    resource resource_data_lake_storage_blob_services 'blobServices' = {
        name: 'default'
        properties: {
            deleteRetentionPolicy: {
                allowPermanentDelete: false
                enabled: true
                days: 90
            }
            containerDeleteRetentionPolicy: {
                enabled: true
                days: 90
            }
        }
    }
}

resource resource_data_lake_storage_container_archive 'Microsoft.Storage/storageAccounts/blobServices/containers@2022-09-01' = {
    name: '${dataLakeStorageName}/default/archive'
    properties: {
        publicAccess: 'None'
    }
    dependsOn: [
       resource_data_lake_storage
   ]
}

resource resource_data_lake_storage_container_databricks 'Microsoft.Storage/storageAccounts/blobServices/containers@2022-09-01' = {
    name: '${dataLakeStorageName}/default/databricks'
    properties: {
        publicAccess: 'None'
    }
    dependsOn: [
       resource_data_lake_storage
   ]
}

resource resource_data_lake_storage_container_datahub 'Microsoft.Storage/storageAccounts/blobServices/containers@2022-09-01' = {
    name: '${dataLakeStorageName}/default/datahub'
    properties: {
        publicAccess: 'None'
    }
    dependsOn: [
       resource_data_lake_storage
   ]
}

resource resource_data_lake_storage_container_ingest 'Microsoft.Storage/storageAccounts/blobServices/containers@2022-09-01' = {
    name: '${dataLakeStorageName}/default/ingest'
    properties: {
        publicAccess: 'None'
    }
    dependsOn: [
       resource_data_lake_storage
   ]
}

resource resource_data_lake_storage_container_synapse 'Microsoft.Storage/storageAccounts/blobServices/containers@2022-09-01' = {
    name: '${dataLakeStorageName}/default/synapse'
    properties: {
        publicAccess: 'None'
    }
    dependsOn: [
       resource_data_lake_storage
   ]
}

resource resource_data_lake_storage_databrics_app_access 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
    name: guid(subscription().id, dataLakeStorageName, 'Storage Blob Data Contributor', databricksManagedAppObjectId)
    scope: resource_data_lake_storage
    properties: {
        principalId: databricksManagedAppObjectId
        principalType: 'ServicePrincipal'
        roleDefinitionId: resourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
    }
}

resource resource_synapse 'Microsoft.Synapse/workspaces@2021-06-01' = {
    location: location
    name: synapseWorkspaceName
    identity: {
        type: 'SystemAssigned'
    }
    properties: {
        defaultDataLakeStorage: {
            resourceId: resource_data_lake_storage.id
            accountUrl: reference(dataLakeStorageName).primaryEndpoints.dfs
            filesystem: 'synapse'
            createManagedPrivateEndpoint: true
        }
        managedVirtualNetwork: 'default'
        managedResourceGroupName: 'rg-${synapseWorkspaceName}'
        sqlAdministratorLogin: synapseWorkspaceSqlAdministratorLogin
        sqlAdministratorLoginPassword: synapseWorkspaceSqlAdministratorLoginPassword
        publicNetworkAccess: 'Enabled'
        azureADOnlyAuthentication: false
        trustedServiceBypassEnabled: false
    }
    dependsOn: [
        resource_data_lake_storage_container_synapse
    ]

    resource firewall_allow_all 'firewallRules' = {
        name: 'allowAll'
        properties: {
            startIpAddress: '0.0.0.0'
            endIpAddress: '255.255.255.255'
        }
    }

    resource firewall_allow_windows_azure_ips 'firewallRules' = {
        name: 'AllowAllWindowsAzureIps'
        properties: {
            startIpAddress: '0.0.0.0'
            endIpAddress: '0.0.0.0'
        }
    }

    resource managed_identity_sql_control_settings 'managedIdentitySqlControlSettings' = {
        name: 'default'
        properties: {
            grantSqlControlToManagedIdentity: {
                desiredState: 'Enabled'
            }
        }
    }

    resource dedicated_sql_minimal_tls_settings 'dedicatedSQLminimalTlsSettings' = {
        name: 'default'
        properties: {
            minimalTlsVersion: '1.2'
        }
    }
}

resource resource_data_lake_storage_synapse_access 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
    name: guid(subscription().id, dataLakeStorageName, 'Storage Blob Data Contributor', synapseWorkspaceName)
    scope: resource_data_lake_storage
    properties: {
        principalId: resource_synapse.identity.principalId
        principalType: 'ServicePrincipal'
        roleDefinitionId: resourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
    }
}

resource resource_databricks_synapse_access 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
    name: guid(subscription().id, databricksWorkspaceName, 'Owner', synapseWorkspaceName)
    scope: resource_databricks
    properties: {
        principalId: resource_synapse.identity.principalId
        principalType: 'ServicePrincipal'
        roleDefinitionId: resourceId('Microsoft.Authorization/roleDefinitions', '8e3af657-a8ff-443c-a75c-2fe8c4bcb635')
    }
}

@description('Powershell command to authorize Synapse Workspace to Data Lake Storage Firewall')
output powerShell_1 string = 'Select-AzSubscription -SubscriptionId "${subscription().subscriptionId}"; Add-AzStorageAccountNetworkRule -ResourceGroupName "${resourceGroup().name}" -Name "${resource_data_lake_storage.name}" -TenantId "${subscription().tenantId}" -ResourceId "${resource_synapse.id}"'

@description('Powershell command to authorize current user as "Storage Blob Data Contributor" to Data Lake Storage Firewall')
output powerShell_2 string = '$myUserId=az ad signed-in-user show --query id -o tsv; az role assignment create --role "Storage Blob Data Contributor" --assignee $myUserId --scope "${resource_data_lake_storage.id}"'

@description('URL to create secret scope pointing to Key Vault in Databricks')
output databricks_url string = 'https://${resource_databricks.properties.workspaceUrl}/#secrets/createScope'

@description('Value to use on "Scope Name" when creating secret scope pointing to Key Vault in Databricks')
output databricks_url_scopeName string = 'KeyVault'

@description('Value to use on "Manage Principal" when creating secret scope pointing to Key Vault in Databricks')
output databricks_url_managePrincipal string = 'All Users' 

@description('Value to use on "DNS Name" when creating secret scope pointing to Key Vault in Databricks')
output databricks_url_dnsName string = resource_keyvault.properties.vaultUri 

@description('Value to use on "Resource ID" when creating secret scope pointing to Key Vault in Databricks')
output databricks_url_resourceId string = resource_keyvault.id