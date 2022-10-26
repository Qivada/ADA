# Databricks notebook source
# DBTITLE 1,Information
# MAGIC %md
# MAGIC Stage parquet files from archive to Azure Synapse Analytics
# MAGIC 
# MAGIC Required additional libraries:
# MAGIC - None
# MAGIC 
# MAGIC Other requirements:
# MAGIC - Write semantics "Copy" requires Azure Databricks >= 7.0

# COMMAND ----------

# Parameters
try:
  # Archive path e.g. archive/adventureworkslt/address/
  __ARCHIVE_PATH = dbutils.widgets.get("ARCHIVE_PATH")
  
  # Optional: Archive log path e.g. archive/adventureworkslt/customer/log/
  __ARCHIVE_LOG_PATH = __ARCHIVE_PATH + "/log"
  try:
    __ARCHIVE_LOG_PATH = dbutils.widgets.get("ARCHIVE_LOG_PATH")
  except:
    print("Using default archive log path: " + __ARCHIVE_LOG_PATH)
   
  # Target process datetime log path e.g. analytics/datawarehouse/address/temp/
  __TARGET_TEMP_PATH = dbutils.widgets.get("TARGET_TEMP_PATH")
  
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
  # True = ArchiveDatetimeUTC >= lastArchiveDatetimeUTC
  # False = ArchiveDatetimeUTC > lastArchiveDatetimeUTC
  __INCLUDE_PREVIOUS = "False"
  try:
    __INCLUDE_PREVIOUS = dbutils.widgets.get("INCLUDE_PREVIOUS")
  except:
    print("Using default include previous: " + __INCLUDE_PREVIOUS)
    
except:
  raise Exception("Required parameter(s) missing")

# COMMAND ----------

# Import
import sys
from pyspark.sql.functions import lit
from pyspark.sql.utils import AnalysisException
from datetime import datetime
import uuid
import pandas as pd

# Configuration
__SECRET_SCOPE = "KeyVault"
__SECRET_NAME_DATA_LAKE_APP_CLIENT_ID = "App-databricks-id"
__SECRET_NAME_DATA_LAKE_APP_CLIENT_SECRET = "App-databricks-secret"
__SECRET_NAME_DATA_LAKE_APP_CLIENT_TENANT_ID = "App-databricks-tenant-id"
__SECRET_NAME_SYNAPSE_JDBC_CONNECTION_STRING = "Synapse-JDBC-connection-string"
__DATA_LAKE_NAME = dbutils.secrets.get(scope = __SECRET_SCOPE, key = "Storage-Name")

__ARCHIVE_PATH = "abfss://archive@" + __DATA_LAKE_NAME + ".dfs.core.windows.net/" + __ARCHIVE_PATH
__ARCHIVE_LOG_PATH = "abfss://archive@" + __DATA_LAKE_NAME + ".dfs.core.windows.net/" + __ARCHIVE_LOG_PATH
__TARGET_TEMP_PATH = "abfss://synapse@" + __DATA_LAKE_NAME + ".dfs.core.windows.net/" + __TARGET_TEMP_PATH + "/" + str(uuid.uuid4())
__TARGET_LOG_PATH = "abfss://synapse@" + __DATA_LAKE_NAME + ".dfs.core.windows.net/" + __TARGET_LOG_PATH + "/processDatetime/"

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

# Azure Synapse Analytics authentication
__SYNAPSE_JDBC = dbutils.secrets.get(scope = __SECRET_SCOPE, key = __SECRET_NAME_SYNAPSE_JDBC_CONNECTION_STRING)

# COMMAND ----------

# Get process datetimes
lastArchiveDatetimeUTC = None
try:
    # Try to read existing log
    lastArchiveDatetimeUTC = spark.sql("SELECT MAX(ArchiveDatetimeUTC) AS ArchiveDatetimeUTC FROM delta.`" + __TARGET_LOG_PATH + "`").collect()[0][0]
    print("Using existing log with time: " + str(lastArchiveDatetimeUTC))
except AnalysisException as ex:
    # Initiliaze delta as it did not exist
    dfProcessDatetimes = spark.sql("SELECT CAST(date_sub(current_timestamp(), 5) AS timestamp) AS ArchiveDatetimeUTC")
    dfProcessDatetimes.write.format("delta").mode("append").option("mergeSchema", "true").save(__TARGET_LOG_PATH)
    lastArchiveDatetimeUTC = spark.sql("SELECT MAX(ArchiveDatetimeUTC) AS ArchiveDatetimeUTC FROM delta.`" + __TARGET_LOG_PATH + "`").collect()[0][0]
    print("Initiliazed log with time: " + str(lastArchiveDatetimeUTC))
except Exception as ex:
    print("Could not read log")
    print(ex)
    raise

# COMMAND ----------

# Get archive log records where ArchiveDatetimeUTC is greater than lastArchiveDatetimeUTC
dfArchiveLogs = spark.sql(" \
  SELECT * \
  FROM   delta.`" + __ARCHIVE_LOG_PATH + "` \
  WHERE  ArchiveDatetimeUTC " + (__INCLUDE_PREVIOUS == "True" and ">=" or ">") + " CAST('" + str(lastArchiveDatetimeUTC) + "' AS timestamp) \
  ORDER BY ArchiveDatetimeUTC ASC \
")

__EXTRACT_COLUMNS = __EXTRACT_COLUMNS.replace('[','`').replace(']','`')
__TABLE_NAME = __TABLE_NAME.replace('[','').replace(']','')

processLogs = []
dfAnalytics = None
dfStaticArchiveLogs = dfArchiveLogs.collect()
for archiveLog in dfStaticArchiveLogs:
    print("Processing file: " + archiveLog.ArchiveFilePath)  
    processLogs.append({
      'ProcessDatetime': datetime.utcnow(),
      'ArchiveDatetimeUTC': archiveLog.ArchiveDatetimeUTC,
      'OriginalStagingFilePath': archiveLog.OriginalStagingFilePath,
      'OriginalStagingFileName': archiveLog.OriginalStagingFileName,
      'OriginalStagingFileSize': archiveLog.OriginalStagingFileSize,
      'ArchiveFilePath': archiveLog.ArchiveFilePath,
      'ArchiveFileName': archiveLog.ArchiveFileName
    })
  
    try:
        # Select from archive and save to target
        dfArchive = spark.sql(" \
          SELECT " + __EXTRACT_COLUMNS + " " + " \
          FROM   parquet.`" + archiveLog.ArchiveFilePath + "` \
        ").withColumn('__ArchiveDatetimeUTC', lit(archiveLog.ArchiveDatetimeUTC)) \
          .withColumn('__OriginalStagingFileName', lit(archiveLog.OriginalStagingFileName))

        dfArchive.write.mode("append").parquet(__TARGET_TEMP_PATH)      
    except:
        print("Could not process file.")
    
if dfStaticArchiveLogs:
    dfAnalytics = spark.read.option("mergeSchema", "true").parquet(__TARGET_TEMP_PATH)
  
    if str(__MAX_STRING_LENGTH).upper() != "MAX":
        print("Writing with optimized SQL connection")
        dfAnalytics.write \
                   .format("com.databricks.spark.sqldw") \
                   .option("url", __SYNAPSE_JDBC) \
                   .option("forwardSparkAzureStorageCredentials", "false") \
                   .option("useAzureMSI", "true") \
                   .mode("overwrite") \
                   .option("maxStrLength", __MAX_STRING_LENGTH) \
                   .option("tableOptions", "DISTRIBUTION = " + __DISTRIBUTION + ", HEAP") \
                   .option("dbTable", __TABLE_NAME) \
                   .option("tempDir", "abfss://databricks@" + __DATA_LAKE_NAME + ".dfs.core.windows.net/temp") \
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

    dbutils.fs.rm(__TARGET_TEMP_PATH, True)

# COMMAND ----------

if processLogs:
    dfProcessLogs = spark.createDataFrame(pd.DataFrame(processLogs)) \
                       .selectExpr("CAST(ProcessDatetime AS timestamp) AS ProcessDatetime", \
                                   "CAST(ArchiveDatetimeUTC AS timestamp) AS ArchiveDatetimeUTC", \
                                   "CAST(OriginalStagingFilePath AS string) AS OriginalStagingFilePath", \
                                   "CAST(OriginalStagingFileName AS string) AS OriginalStagingFileName", \
                                   "CAST(OriginalStagingFileSize AS long) AS OriginalStagingFileSize", \
                                   "CAST(ArchiveFilePath AS string) AS ArchiveFilePath", \
                                   "CAST(ArchiveFileName AS string) AS ArchiveFileName")
    dfProcessLogs.write.format("delta") \
                     .mode("append") \
                     .option("mergeSchema", "true") \
                     .save(__TARGET_LOG_PATH) 
  
    print('Optimize log delta: ' + __TARGET_LOG_PATH)
    spark.sql('OPTIMIZE delta.`' + __TARGET_LOG_PATH + '`').display()

# COMMAND ----------

# Return success
dbutils.notebook.exit(True)
