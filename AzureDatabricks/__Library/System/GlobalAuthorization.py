# Databricks notebook source
# MAGIC %md
# MAGIC Cluster Spark configuration example:
# MAGIC - fs.azure.account.auth.type.`<storage-account-name>`.dfs.core.windows.net OAuth
# MAGIC - fs.azure.account.oauth.provider.type.`<storage-account-name>`.dfs.core.windows.net org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider
# MAGIC - fs.azure.account.oauth2.client.id.`<storage-account-name>`.dfs.core.windows.net `<application-id>`
# MAGIC - fs.azure.account.oauth2.client.secret.`<storage-account-name>`.dfs.core.windows.net `<service-credential>`
# MAGIC - fs.azure.account.oauth2.client.endpoint.`<storage-account-name>`.dfs.core.windows.net https://login.microsoftonline.com/ `<directory-id>`/oauth2/token
