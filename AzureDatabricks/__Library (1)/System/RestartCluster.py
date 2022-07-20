# Databricks notebook source
# DBTITLE 1,Information
# MAGIC %md
# MAGIC Restart cluster
# MAGIC 
# MAGIC Required additional libraries:
# MAGIC - None

# COMMAND ----------

# DBTITLE 1,Configure
import requests

# Locate from browser URL e.g. https://[databricksInstance].azuredatabricks.net/
databricksInstance = '[REMOVED]'

# Create access token from user settings
accessToken = '[REMOVED]'

# Locate from browser URL after opening cluster configuration e.g. https://adb-5837306691210730.10.azuredatabricks.net/?o=5837306691210730#/setting/clusters/[clusterId]/configuration
clusterId = '[REMOVED]'

# COMMAND ----------

# DBTITLE 1,Restart cluster
requests.post('https://' + databricksInstance + '.azuredatabricks.net/api/2.0/clusters/restart', 
              headers={
                'x-ms-version': '2019-07-07',
                'Authorization': 'Bearer ' + accessToken + '',
                'Content-Type': 'application/json'
              }, 
              data="{\"cluster_id\": \"" + clusterId + "\"}")
