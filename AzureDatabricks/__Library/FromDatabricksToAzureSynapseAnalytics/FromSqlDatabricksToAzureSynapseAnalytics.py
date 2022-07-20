# Databricks notebook source
# DBTITLE 1,Information
# MAGIC %md
# MAGIC Stage sql query data from Databricks to Azure Synapse Analytics
# MAGIC 
# MAGIC Required additional libraries:
# MAGIC - None
# MAGIC 
# MAGIC Other requirements:
# MAGIC - Write semantics "Copy" requires Azure Databricks >= 7.0

# COMMAND ----------

# Parameters
try:
  # Source sql e.g. SELECT * FROM lehdetdw.f_tilaus
  __SOURCE_SQL = dbutils.widgets.get("SOURCE_SQL")
  
  # Source track date columns e.g. __ModifiedDatetimeUTC
  __SOURCE_TRACK_DATE_COLUMN = "__ModifiedDatetimeUTC"
  try:
    __SOURCE_TRACK_DATE_COLUMN = dbutils.widgets.get("SOURCE_TRACK_DATE_COLUMN")
  except:
    print("Using default source track column: " + __SOURCE_TRACK_DATE_COLUMN)
   
  # Target process datetime log path e.g. analytics/datawarehouse/address/log/
  __TARGET_LOG_PATH = dbutils.widgets.get("TARGET_LOG_PATH")
  
  # Columns to extract e.g. * or AddressID, AddressLine1, AddressLine2, City, StateProvince, CountryRegion, PostalCode, rowguid, ModifiedDate
  __EXTRACT_COLUMNS = dbutils.widgets.get("EXTRACT_COLUMNS")
  
  # Table name with schema e.g. stg.X_adventureworkslt_address
  __TABLE_NAME = dbutils.widgets.get("TABLE_NAME")
  
  # Target table distribution e.g ROUNDROBIN or HASH(AddressID)
  __DISTRIBUTION = "ROUND_ROBIN"
  try:
    __DISTRIBUTION = dbutils.widgets.get("DISTRIBUTION")
  except:
    print("Using default distribution: " + __DISTRIBUTION)
    
  # Max. string length e.g. 250
  __MAX_STRING_LENGTH = 250
  try:
    __MAX_STRING_LENGTH = dbutils.widgets.get("MAX_STRING_LENGTH")
  except:
    print("Using default max. string length: " + str(__MAX_STRING_LENGTH))
    
  # Include previous. Use "True" or "False"
  # True = ArchiveDatetimeUTC >= lastProcessDatetimeUTC
  # False = ArchiveDatetimeUTC > lastProcessDatetimeUTC
  __INCLUDE_PREVIOUS = "False"
  try:
    __INCLUDE_PREVIOUS = dbutils.widgets.get("INCLUDE_PREVIOUS")
  except:
    print("Using default include previous: " + __INCLUDE_PREVIOUS)
    
  # Delta day count e.g. get data newer than 3 days since the last processing date
  __DELTA_DAY_COUNT = 0
  try:
    __DELTA_DAY_COUNT = dbutils.widgets.get("DELTA_DAY_COUNT")
  except:
    print("Using default delta day count: " + str(__DELTA_DAY_COUNT))
    
except:
  raise Exception("Required parameter(s) missing")

# COMMAND ----------

# Import
import sys
from pyspark.sql.functions import lit
from pyspark.sql.utils import AnalysisException
from datetime import datetime, timedelta

# Configuration
__SECRET_SCOPE = "KeyVault"
__SECRET_NAME_DATA_LAKE_APP_CLIENT_ID = "App--ADA-Lab--id"
__SECRET_NAME_DATA_LAKE_APP_CLIENT_SECRET = "App--ADA-Lab--secret"
__SECRET_NAME_DATA_LAKE_APP_CLIENT_TENANT_ID = "App--ADA-Lab--tenant-id"
__SECRET_NAME_BLOB_ACCOUNT = "blob-account"
__SECRET_NAME_BLOB_ACCOUNT_KEY = "blob-account-key"
__SECRET_NAME_BLOB_TEMP_CONTAINER = "blob-temp-container"
__SECRET_NAME_SYNAPSE_JDBC_CONNECTION_STRING = "Synapse-JDBC-connection-string"
__DATA_LAKE_NAME = dbutils.secrets.get(scope = __SECRET_SCOPE, key = "Storage-Name")
__DATA_LAKE_URL = dbutils.secrets.get(scope = __SECRET_SCOPE, key = "Storage-URL")

__TARGET_LOG_PATH = __DATA_LAKE_URL + "/" + __TARGET_LOG_PATH + "/processDatetime/"

# Data lake authentication
spark.conf.set("fs.azure.account.auth.type." + __DATA_LAKE_NAME + ".dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type." + __DATA_LAKE_NAME + ".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id." + __DATA_LAKE_NAME + ".dfs.core.windows.net", dbutils.secrets.get(scope = __SECRET_SCOPE, key = __SECRET_NAME_DATA_LAKE_APP_CLIENT_ID))
spark.conf.set("fs.azure.account.oauth2.client.secret." + __DATA_LAKE_NAME + ".dfs.core.windows.net", dbutils.secrets.get(scope = __SECRET_SCOPE, key = __SECRET_NAME_DATA_LAKE_APP_CLIENT_SECRET))
spark.conf.set("fs.azure.account.oauth2.client.endpoint." + __DATA_LAKE_NAME + ".dfs.core.windows.net", "https://login.microsoftonline.com/" + dbutils.secrets.get(scope = __SECRET_SCOPE, key = __SECRET_NAME_DATA_LAKE_APP_CLIENT_TENANT_ID) + "/oauth2/token")

# Configure the write semantics for Azure Synapse connector in the notebook session configuration:
# polybase (use PolyBase for loading data into Azure Synapse)
# copy (In Databricks Runtime 7.0 and above, use the COPY statement to load data into Azure Synapse)
# unspecified (falls back to default: for ADLS Gen2 on Databricks Runtime 7.0 and above the connector will use copy, else polybase)
spark.conf.set("spark.databricks.sqldw.writeSemantics", "copy")

# In Spark 3.1, loading and saving of timestamps from/to parquet files fails if the timestamps are before 1900-01-01 00:00:00Z, and loaded (saved) as the INT96 type. 
# In Spark 3.0, the actions don’t fail but might lead to shifting of the input timestamps due to rebasing from/to Julian to/from Proleptic Gregorian calendar. 
# To restore the behavior before Spark 3.1, you can set spark.sql.legacy.parquet.int96RebaseModeInRead or/and spark.sql.legacy.parquet.int96RebaseModeInWrite to LEGACY.
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "LEGACY")

# Blob storage (tempDir) authentication
__BLOB_STORAGE_ACCOUNT = dbutils.secrets.get(scope = __SECRET_SCOPE, key = __SECRET_NAME_BLOB_ACCOUNT)
__BLOB_STORAGE_KEY = dbutils.secrets.get(scope = __SECRET_SCOPE, key = __SECRET_NAME_BLOB_ACCOUNT_KEY)
__BLOB_TEMP_CONTAINER = dbutils.secrets.get(scope = __SECRET_SCOPE, key = __SECRET_NAME_BLOB_TEMP_CONTAINER)
spark.conf.set("fs.azure.account.key." + __BLOB_STORAGE_ACCOUNT + ".blob.core.windows.net", __BLOB_STORAGE_KEY)

# Azure Synapse Analytics authentication
__SYNAPSE_JDBC = dbutils.secrets.get(scope = __SECRET_SCOPE, key = __SECRET_NAME_SYNAPSE_JDBC_CONNECTION_STRING)

# COMMAND ----------

# Get process datetimes
lastProcessDatetimeUTC = None
try:
  dfProcessDatetimes = spark.read.format("delta").load(__TARGET_LOG_PATH)
  lastProcessDatetimeUTC = dfProcessDatetimes.select("ProcessDatetime").rdd.max()[0] - timedelta(days = int(__DELTA_DAY_COUNT))
  print("Using existing log with date: " + str(lastProcessDatetimeUTC))
except AnalysisException as ex:
  # Initiliaze delta as it did not exist
  dfProcessDatetimes = spark.sql("SELECT CAST('1900-01-01' AS timestamp) AS ProcessDatetime")
  dfProcessDatetimes.write.format("delta").mode("append").option("mergeSchema", "true").save(__TARGET_LOG_PATH)
  lastProcessDatetimeUTC = dfProcessDatetimes.select("ProcessDatetime").rdd.max()[0]
  print("Initiliazed log with date: " + str(lastProcessDatetimeUTC))
except Exception as ex:
  print("Could not read log")
  print(ex)
  raise

# COMMAND ----------

# Use following option if Azure Synapse Analytics is NOT USING Managed Service Identity to connect to Azure Storage
# .option("forwardSparkAzureStorageCredentials", "true") \

# Use following option if Azure Synapse Analytics IS USING Managed Service Identity to connect to Azure Storage
# .option("useAzureMSI", "true") \ 

# See details:
# https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/azure/synapse-analytics#azure-synapse-to-azure-storage-account

queryBeginsDatetimeUTC = datetime.utcnow()
dfAnalytics = spark.sql("SELECT * FROM (" + __SOURCE_SQL + ") AS sourceSQL WHERE `" + __SOURCE_TRACK_DATE_COLUMN + "` " + (__INCLUDE_PREVIOUS == "True" and ">=" or ">") + " CAST('" + str(lastProcessDatetimeUTC) + "' AS timestamp)")

if str(__MAX_STRING_LENGTH).upper() != "MAX":
  print("Writing with optimized SQL connection")
  dfAnalytics.write \
             .format("com.databricks.spark.sqldw") \
             .option("url", __SYNAPSE_JDBC) \
             .option("forwardSparkAzureStorageCredentials", "true") \
             .mode("overwrite") \
             .option("maxStrLength", __MAX_STRING_LENGTH) \
             .option("tableOptions", "DISTRIBUTION = " + __DISTRIBUTION + ", HEAP") \
             .option("dbTable", __TABLE_NAME) \
             .option("tempDir", "wasbs://" + __BLOB_TEMP_CONTAINER + "@" + __BLOB_STORAGE_ACCOUNT + ".blob.core.windows.net/databricks") \
             .save()
else:
  print("Writing with optimized SQL connection. Using nvarchar(max)")
  df_schema = spark.createDataFrame([], dfAnalytics.schema)
  df_schema.write \
           .option("createTableOptions", "WITH(DISTRIBUTION = " + __DISTRIBUTION + ", HEAP)" ) \
           .option("batchsize", 1 ) \
           .jdbc(url=__SYNAPSE_JDBC, table=__TABLE_NAME, mode="overwrite")
  dfAnalytics.write \
             .format("com.databricks.spark.sqldw") \
             .option("url", __SYNAPSE_JDBC) \
             .option("forwardSparkAzureStorageCredentials", "false") \
             .option("useAzureMSI", "true") \
             .mode("append") \
             .option("tableOptions", "DISTRIBUTION = " + __DISTRIBUTION + ", HEAP") \
             .option("dbTable", __TABLE_NAME) \
             .option("tempDir", "abfss://databricks@" + __DATA_LAKE_NAME + ".dfs.core.windows.net/temp") \
             .save()

# COMMAND ----------

# Check if anything was done
maxDatetimeUTC = spark.sql("""
  SELECT MAX(`""" + __SOURCE_TRACK_DATE_COLUMN + """`) AS `maxDatetimeUTC` 
  FROM (""" + __SOURCE_SQL + """) AS sourceSQL
  WHERE `""" + __SOURCE_TRACK_DATE_COLUMN + """` >= CAST('""" + str(lastProcessDatetimeUTC) + """' AS timestamp) AND
        `""" + __SOURCE_TRACK_DATE_COLUMN + """` <= CAST('""" + str(queryBeginsDatetimeUTC) + """' AS timestamp)""").rdd.max()[0]

if maxDatetimeUTC:
  # Save max. archive datetime to target process datetime log
  dfProcessDatetime = spark.sql("SELECT CAST('" + str(maxDatetimeUTC) + "' AS timestamp) AS ProcessDatetime")
  dfProcessDatetime.write.format("delta") \
                         .mode("append") \
                         .save(__TARGET_LOG_PATH)

# COMMAND ----------

# Return success
dbutils.notebook.exit(True)
