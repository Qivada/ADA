// =========================================== 
// Metadata
// ===========================================
metadata author = 'Qivada'
metadata changeHistory = [
    {
        date: '2023-03-16'
        change: 'Initial version'
    }
]

// =========================================== 
// Parameters
// ===========================================
@description('Location for all resources.')
param location string = resourceGroup().location

@description('Name of the NAT gateway to be attached to the workspace subnets.')
param databricksNatGatewayName string = 'nat-PROJECT-dbw-ENVIRONMENT-REGION-INSTANCE'

@description('Name of the NAT gateway public IP.')
param databricksNatGatewayPublicIpName string = 'pip-PROJECT-dbw-ENVIRONMENT-REGION-INSTANCE'

@description('The name of the network security group to create.')
param databricksNsgName string = 'nsg-PROJECT-dbw-ENVIRONMENT-REGION-INSTANCE'

@description('The pricing tier of workspace.')
@allowed([
  'trial'
  'standard'
  'premium'
])
param databricksPricingTier string = 'premium'

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

// =========================================== 
// Variables
// ===========================================
var var_managedResourceGroupName = 'databricks-rg-${databricksWorkspaceName}-${uniqueString(databricksWorkspaceName, resourceGroup().id)}'
var var_trimmedMRGName = substring(var_managedResourceGroupName, 0, min(length(var_managedResourceGroupName), 90))
var var_managedResourceGroupId = '${subscription().id}/resourceGroups/${var_trimmedMRGName}'
var var_databricksNsgId = resourceId('Microsoft.Network/networkSecurityGroups/', databricksNsgName)
var var_databricksVnetId = resourceId('Microsoft.Network/virtualNetworks/', databricksVnetName)
var var_databricksNatId = resourceId('Microsoft.Network/natGateways/', databricksNatGatewayName)

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
        name: databricksPricingTier
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