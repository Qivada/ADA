# Register Application

1. Open [Register an application](https://portal.azure.com/#view/Microsoft_AAD_RegisteredApps/CreateApplicationBlade/quickStartType~/null/isMSAApp~/false) dialog (Azure Actice Directory > App registrations > New registration)
   - Name. Use naming standard e.g. app-PROJECT-USAGE-ENVIRONMENT
   - Supported account types. Select 'Accounts in this organizational directory only (Qivada only - Single tenant)'
   - Leave rest with default values and select 'Register'
   ![image](https://user-images.githubusercontent.com/109618668/225613213-04707967-7557-4bb9-ba8b-cdfdf42868fd.png)

2. Open registered application and collect following information:
   - Application (client) ID and Directory (tenant) ID
   ![image](https://user-images.githubusercontent.com/109618668/225612462-b688254c-8fef-4f06-9845-d15504ac59bd.png)

3. From registered application navigate to managed application (enterprise application) and collect following information:
   - Object ID
   ![image](https://user-images.githubusercontent.com/109618668/225617999-a4fae278-b456-4b30-8baa-d9d503b61d95.png)
   ![image](https://user-images.githubusercontent.com/109618668/225614069-178e9f7b-8b59-4fc2-b3ec-0ac6d9558408.png)
   
4. Open registered application and create secret. Collect following information:
   - Expire and Value
   ![image](https://user-images.githubusercontent.com/109618668/225614771-05f64e18-2ae3-4f41-be15-d09180ba0e66.png)
   ![image](https://user-images.githubusercontent.com/109618668/225615087-6e5a8725-15e0-4925-a079-70ab360a2359.png)
   ![image](https://user-images.githubusercontent.com/109618668/225615428-5f6e8081-7edd-45de-8837-6ec8c70648d0.png)

5. Application registration is now complete. You should have following information collected:
   - Application (client) ID and Directory (tenant) ID from application registration
   - Object ID from managed application (enterprise application)
   - Expire and value from application registration's certificates & secrets
