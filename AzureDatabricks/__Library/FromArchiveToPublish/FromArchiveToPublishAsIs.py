# Databricks notebook source
# DBTITLE 1,Information
# MAGIC %md
# MAGIC Copy files from archive to target folder
# MAGIC
# MAGIC Required additional libraries:
# MAGIC - None

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
  
    # Target path e.g. analytics/datawarehouse/address/
    __TARGET_PATH = dbutils.widgets.get("TARGET_PATH")
  
    # Target process datetime log path e.g. analytics/datawarehouse/address/log/
    __TARGET_LOG_PATH = dbutils.widgets.get("TARGET_LOG_PATH")
  
    # Target file extension e.g. .parquet
    __TARGET_FILE_EXTENSION = dbutils.widgets.get("TARGET_FILE_EXTENSION")
  
    # Clear target. Use "True" or "False"
    __CLEAR_TARGET = dbutils.widgets.get("CLEAR_TARGET")
  
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
from pyspark.sql.utils import AnalysisException
from datetime import datetime
import pandas as pd

# Configuration
__SECRET_SCOPE = "KeyVault"
__SECRET_NAME_DATA_LAKE_APP_CLIENT_ID = "App-databricks-id"
__SECRET_NAME_DATA_LAKE_APP_CLIENT_SECRET = "App-databricks-secret"
__SECRET_NAME_DATA_LAKE_APP_CLIENT_TENANT_ID = "App-databricks-tenant-id"
__DATA_LAKE_NAME = dbutils.secrets.get(scope = __SECRET_SCOPE, key = "Storage-Name")

__ARCHIVE_PATH = "abfss://archive@" + __DATA_LAKE_NAME + ".dfs.core.windows.net/" + __ARCHIVE_PATH
__ARCHIVE_LOG_PATH = "abfss://archive@" + __DATA_LAKE_NAME + ".dfs.core.windows.net/" + __ARCHIVE_LOG_PATH
__TARGET_PATH = "abfss://datahub@" + __DATA_LAKE_NAME + ".dfs.core.windows.net/" + __TARGET_PATH
__TARGET_LOG_PATH = "abfss://datahub@" + __DATA_LAKE_NAME + ".dfs.core.windows.net/" + __TARGET_LOG_PATH + "/processDatetime/"

# In Spark 3.1, loading and saving of timestamps from/to parquet files fails if the timestamps are before 1900-01-01 00:00:00Z, and loaded (saved) as the INT96 type. 
# In Spark 3.0, the actions don’t fail but might lead to shifting of the input timestamps due to rebasing from/to Julian to/from Proleptic Gregorian calendar. 
# To restore the behavior before Spark 3.1, you can set spark.sql.parquet.int96RebaseModeInRead or/and spark.sql.legacy.parquet.int96RebaseModeInWrite to LEGACY.
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")

# Data lake authentication
spark.conf.set("fs.azure.account.auth.type." + __DATA_LAKE_NAME + ".dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type." + __DATA_LAKE_NAME + ".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id." + __DATA_LAKE_NAME + ".dfs.core.windows.net", dbutils.secrets.get(scope = __SECRET_SCOPE, key = __SECRET_NAME_DATA_LAKE_APP_CLIENT_ID))
spark.conf.set("fs.azure.account.oauth2.client.secret." + __DATA_LAKE_NAME + ".dfs.core.windows.net", dbutils.secrets.get(scope = __SECRET_SCOPE, key = __SECRET_NAME_DATA_LAKE_APP_CLIENT_SECRET))
spark.conf.set("fs.azure.account.oauth2.client.endpoint." + __DATA_LAKE_NAME + ".dfs.core.windows.net", "https://login.microsoftonline.com/" + dbutils.secrets.get(scope = __SECRET_SCOPE, key = __SECRET_NAME_DATA_LAKE_APP_CLIENT_TENANT_ID) + "/oauth2/token")

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

# Clear target
try:
    if __CLEAR_TARGET == "True":
        dfTargetFiles = dbutils.fs.ls(__TARGET_PATH)
        for targetFile in dfTargetFiles:
            # Target must not be folder (path ends with / e.g. Log/)
            if targetFile.path.endswith("/") == False:
                dbutils.fs.rm(targetFile.path)
                print("Removed target file '" + targetFile.path + "'.")
    else:
        print("Target was not cleared.")
except:
    print("Clear target failed. Probably target does not exists.")

# COMMAND ----------

# Get archive log records where ArchiveDatetimeUTC is greater than lastArchiveDatetimeUTC
try:
    dfArchiveLogs = spark.sql(" \
      SELECT * \
      FROM   delta.`" + __ARCHIVE_LOG_PATH + "` \
      WHERE  ArchiveDatetimeUTC " + (__INCLUDE_PREVIOUS == "True" and ">=" or ">") + " CAST('" + str(lastArchiveDatetimeUTC) + "' AS timestamp) AND `IsPurged` = 0 AND `IsIgnorable` = 0 \
      ORDER BY ArchiveDatetimeUTC ASC, OriginalModificationTime ASC \
    ")
except:
    # Failsafe without OriginalModificationTime that was included later on to archive log
    dfArchiveLogs = spark.sql(" \
      SELECT * \
      FROM   delta.`" + __ARCHIVE_LOG_PATH + "` \
      WHERE  ArchiveDatetimeUTC " + (__INCLUDE_PREVIOUS == "True" and ">=" or ">") + " CAST('" + str(lastArchiveDatetimeUTC) + "' AS timestamp) AND `IsPurged` = 0 AND `IsIgnorable` = 0 \
      ORDER BY ArchiveDatetimeUTC ASC \
    ")

processLogs = []
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
  
    # Copy archived file into target
    dbutils.fs.cp(archiveLog.ArchiveFilePath, __TARGET_PATH + "/" + archiveLog.ArchiveFileName + __TARGET_FILE_EXTENSION)

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
