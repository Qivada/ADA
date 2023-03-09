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
param param_location string = resourceGroup().location

@description('Name of the NAT gateway to be attached to the workspace subnets.')
param param_databricksNatGatewayName string = 'nat-PROJECT-dbw-ENVIRONMENT-REGION-INSTANCE'

@description('Name of the NAT gateway public IP.')
param param_databricksNatGatewayPublicIpName string = 'pip-PROJECT-dbw-ENVIRONMENT-REGION-INSTANCE'

@description('The name of the network security group to create.')
param param_databricksNsgName string = 'nsg-PROJECT-dbw-ENVIRONMENT-REGION-INSTANCE'

@description('The pricing tier of workspace.')
@allowed([
  'trial'
  'standard'
  'premium'
])
param param_databricksPricingTier string = 'premium'

@description('The name of the virtual network to create.')
param param_databricksVnetName string = 'vnet-PROJECT-ENVIRONMENT-REGION-INSTANCE'

@description('Cidr range for the vnet.')
param param_databricksVnetCidr string = '10.1.0.0/21'

@description('The name of the public subnet to create.')
param param_databricksPublicSubnetName string = 'snet-PROJECT-dbw-public-ENVIRONMENT-REGION-INSTANCE'

@description('Cidr range for the public subnet.')
param param_databricksPublicSubnetCidr string = '10.1.0.0/24'

@description('The name of the private subnet to create.')
param param_databricksPrivateSubnetName string = 'snet-PROJECT-dbw-private-ENVIRONMENT-REGION-INSTANCE'

@description('Cidr range for the private subnet.')
param param_databricksPrivateSubnetCidr string = '10.1.1.0/24'

@description('The name of the Azure Databricks workspace to create.')
param param_databricksWorkspaceName string = 'dbw-PROJECT-ENVIRONMENT-REGION-INSTANCE'

@description('The name of the Azure Key Vault to create.')
@minLength(3)
@maxLength(24)
param param_keyVaultName string = 'kv-PROJECT-ENVIRONMENT-REGION-INSTANCE'

// =========================================== 
// Variables
// ===========================================
var var_managedResourceGroupName = 'databricks-rg-${param_databricksWorkspaceName}-${uniqueString(param_databricksWorkspaceName, resourceGroup().id)}'
var var_trimmedMRGName = substring(var_managedResourceGroupName, 0, min(length(var_managedResourceGroupName), 90))
var var_managedResourceGroupId = '${subscription().id}/resourceGroups/${var_trimmedMRGName}'
var var_databricksNsgId = resourceId('Microsoft.Network/networkSecurityGroups/', param_databricksNsgName)
var var_databricksVnetId = resourceId('Microsoft.Network/virtualNetworks/', param_databricksVnetName)
var var_databricksNatId = resourceId('Microsoft.Network/natGateways/', param_databricksNatGatewayName)
var var_databricksNatPipId = resourceId('Microsoft.Network/publicIPAddresses/', param_databricksNatGatewayPublicIpName)

// =========================================== 
// Resources
// ===========================================
resource resource_databricks_nat_pip 'Microsoft.Network/publicIPAddresses@2022-09-01' = {
    location: param_location
    name: param_databricksNatGatewayPublicIpName
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
    location: param_location
    name: param_databricksNatGatewayName
    sku: {
        name: 'Standard'
    }
    properties: {
        idleTimeoutInMinutes: 4
        publicIpAddresses: [
            {
                id: resourceId('Microsoft.Network/publicIPAddresses/', param_databricksNatGatewayPublicIpName)
            }
        ]
    }
    dependsOn: [
        resource_databricks_nat_pip
    ]
}

resource resource_databricks_nsg 'Microsoft.Network/networkSecurityGroups@2022-09-01' = {
    location: param_location
    name: param_databricksNsgName
}

resource resource_databricks_vnet 'Microsoft.Network/virtualNetworks@2022-09-01' = {
    location: param_location
    name: param_databricksVnetName
    properties: {
        addressSpace: {
            addressPrefixes: [
                param_databricksVnetCidr
            ]
        }
        subnets: [
            {
                name: param_databricksPublicSubnetName
                properties: {
                    addressPrefix: param_databricksPublicSubnetCidr
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
                name: param_databricksPrivateSubnetName
                properties: {
                    addressPrefix: param_databricksPrivateSubnetCidr
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
    location: param_location
    name: param_databricksWorkspaceName
    sku: {
        name: param_databricksPricingTier
    }
    properties: {
        #disable-next-line use-resource-id-functions
        managedResourceGroupId: var_managedResourceGroupId
        parameters: {
            customVirtualNetworkId: {
                value: var_databricksVnetId
            }
            customPublicSubnetName: {
                value: param_databricksPublicSubnetName
            }
            customPrivateSubnetName: {
                value: param_databricksPrivateSubnetName
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
    location: param_location
    name: param_keyVaultName
    properties: {
        enabledForDeployment: false
        enabledForDiskEncryption: false
        enabledForTemplateDeployment: false
        enableSoftDelete: true
        softDeleteRetentionInDays: 90
        enablePurgeProtection: true
        enableRbacAuthorization: true
        tenantId: subscription().tenantId
        sku: {
            family: 'A'
            name: 'standard'
        }
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
                    id: '${var_databricksVnetId}/subnets/${param_databricksPublicSubnetName}'
                    ignoreMissingVnetServiceEndpoint: false
                }
            ]
        }
        publicNetworkAccess: 'Enabled'
    }
    dependsOn: [
        resource_databricks_vnet
        resource_databricks_nat_pip
        resource_databricks
    ]
}