# Databricks notebook source
# DBTITLE 1,Information
# MAGIC %md
# MAGIC Manage secret scopes
# MAGIC 
# MAGIC Required additional libraries:
# MAGIC - None
# MAGIC 
# MAGIC See secrets API documentation from https://docs.microsoft.com/en-us/azure/databricks/dev-tools/api/latest/secrets

# COMMAND ----------

# DBTITLE 1,Configure
import requests
import json

# Locate from browser URL e.g. https://[databricksInstance].azuredatabricks.net/
databricksInstance = '[REMOVED]'

# Create access token from user settings
accessToken = '[REMOVED]'

# Locate from browser URL after opening cluster configuration e.g. https://adb-5837306691210730.10.azuredatabricks.net/?o=5837306691210730#/setting/clusters/[clusterId]/configuration
clusterId = '[REMOVED]'


# COMMAND ----------

# DBTITLE 1,List secret scopes
response = requests.get('https://' + databricksInstance + '.azuredatabricks.net/api/2.0/secrets/scopes/list',
              headers={
                'x-ms-version': '2019-07-07',
                'Authorization': 'Bearer ' + accessToken + '',
                'Content-Type': 'application/json'
              })

try:
  print(json.dumps(json.loads(response.text), indent=4, sort_keys=True))
except Exception as e:
  print(e)

# COMMAND ----------

# DBTITLE 1,Show ACLs of a secret scope
# Secret scope
__SECRET_SCOPE = "[REMOVED]"

response = requests.get('https://' + databricksInstance + '.azuredatabricks.net/api/2.0/secrets/acls/list',
              headers={
                'x-ms-version': '2019-07-07',
                'Authorization': 'Bearer ' + accessToken + '',
                'Content-Type': 'application/json'
              },
              data="{\"scope\": \"" + __SECRET_SCOPE + "\"}")

try:
  print(json.dumps(json.loads(response.text), indent=4, sort_keys=True))
except Exception as e:
  print(e)

# COMMAND ----------

# DBTITLE 1,Add ACL into a secret scope
# Secret scope
__SECRET_SCOPE = "[REMOVED]"

# User, or group. For all users use value: users
__ACL_PRINCIPAL = "users"

# READ, WRITE, MANAGE
__ACL_PERMISSION = "READ"

response = requests.post('https://' + databricksInstance + '.azuredatabricks.net/api/2.0/secrets/acls/put',
              headers={
                'x-ms-version': '2019-07-07',
                'Authorization': 'Bearer ' + accessToken + '',
                'Content-Type': 'application/json'
              },
              data="{\"scope\": \"" + __SECRET_SCOPE + "\", \"principal\": \"" + __ACL_PRINCIPAL + "\", \"permission\": \"" + __ACL_PERMISSION + "\"}")

try:
  print(json.dumps(json.loads(response.text), indent=4, sort_keys=True))
except Exception as e:
  print(e)

# COMMAND ----------

# DBTITLE 1,Delete ACL from a secret scope
# Secret scope
__SECRET_SCOPE = "[REMOVED]"

# User, or group. For all users use value: users
__ACL_PRINCIPAL = "users"

response = requests.post('https://' + databricksInstance + '.azuredatabricks.net/api/2.0/secrets/acls/delete',
              headers={
                'x-ms-version': '2019-07-07',
                'Authorization': 'Bearer ' + accessToken + '',
                'Content-Type': 'application/json'
              },
              data="{\"scope\": \"" + __SECRET_SCOPE + "\", \"principal\": \"" + __ACL_PRINCIPAL + "\", \"permission\": \"" + __ACL_PERMISSION + "\"}")

try:
  print(json.dumps(json.loads(response.text), indent=4, sort_keys=True))
except Exception as e:
  print(e)
