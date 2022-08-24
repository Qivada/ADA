# Databricks notebook source
# DBTITLE 1,Information
# MAGIC %md
# MAGIC Archive files from blob storage
# MAGIC 
# MAGIC Required additional libraries:
# MAGIC - None

# COMMAND ----------

# Parameters
try:
   # Container e.g. anaplan
  __CONTAINER = dbutils.widgets.get("CONTAINER")
  
  # Ingest path e.g. ingest/adventureworkslt/customer/
  __INGEST_PATH = dbutils.widgets.get("INGEST_PATH")
  
  # Archive path e.g. archive/adventureworkslt/customer/ 
  __ARCHIVE_PATH = dbutils.widgets.get("ARCHIVE_PATH")
  
  # Optional: Archive log path e.g. archive/adventureworkslt/customer/log/
  __ARCHIVE_LOG_PATH = __ARCHIVE_PATH + "/log"
  try:
    __ARCHIVE_LOG_PATH = dbutils.widgets.get("ARCHIVE_LOG_PATH")
  except:
    print("Using default archive log path: " + __ARCHIVE_LOG_PATH)
except:
  raise Exception("Required parameter(s) missing")

# COMMAND ----------

# Import
from pyspark.sql.functions import lit, col
from datetime import datetime
import uuid
import pandas as pd
import os
from joblib import Parallel, delayed, parallel_backend
from pyspark.sql.utils import AnalysisException

# Configuration
__SECRET_SCOPE = "KeyVault"
__SECRET_NAME_DATA_LAKE_APP_CLIENT_ID = "App-databricks-id"
__SECRET_NAME_DATA_LAKE_APP_CLIENT_SECRET = "App-databricks-secret"
__SECRET_NAME_DATA_LAKE_APP_CLIENT_TENANT_ID = "App-databricks-tenant-id"
__SECRET_NAME_BLOB_ACCOUNT = "blob-account"
__SECRET_NAME_BLOB_ACCOUNT_KEY = "blob-account-key"
__DATA_LAKE_NAME = dbutils.secrets.get(scope = __SECRET_SCOPE, key = "Storage-Name")

__INGEST_PATH = "abfss://ingest@" + __DATA_LAKE_NAME + ".dfs.core.windows.net/" + __INGEST_PATH
__ARCHIVE_PATH = "abfss://archive@" + __DATA_LAKE_NAME + ".dfs.core.windows.net/" + __ARCHIVE_PATH
__ARCHIVE_LOG_PATH = "abfss://archive@" + __DATA_LAKE_NAME + ".dfs.core.windows.net/" + __ARCHIVE_LOG_PATH

# Source blob storage authentication
__BLOB_STORAGE_ACCOUNT = dbutils.secrets.get(scope = __SECRET_SCOPE, key = __SECRET_NAME_BLOB_ACCOUNT)
__BLOB_STORAGE_KEY = dbutils.secrets.get(scope = __SECRET_SCOPE, key = __SECRET_NAME_BLOB_ACCOUNT_KEY)
spark.conf.set("fs.azure.account.key." + __BLOB_STORAGE_ACCOUNT + ".blob.core.windows.net", __BLOB_STORAGE_KEY)

# In Spark 3.1, loading and saving of timestamps from/to parquet files fails if the timestamps are before 1900-01-01 00:00:00Z, and loaded (saved) as the INT96 type. 
# In Spark 3.0, the actions don’t fail but might lead to shifting of the input timestamps due to rebasing from/to Julian to/from Proleptic Gregorian calendar. 
# To restore the behavior before Spark 3.1, you can set spark.sql.legacy.parquet.int96RebaseModeInRead or/and spark.sql.legacy.parquet.int96RebaseModeInWrite to LEGACY.
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "LEGACY")

# Data lake authentication
spark.conf.set("fs.azure.account.auth.type." + __DATA_LAKE_NAME + ".dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type." + __DATA_LAKE_NAME + ".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id." + __DATA_LAKE_NAME + ".dfs.core.windows.net", dbutils.secrets.get(scope = __SECRET_SCOPE, key = __SECRET_NAME_DATA_LAKE_APP_CLIENT_ID))
spark.conf.set("fs.azure.account.oauth2.client.secret." + __DATA_LAKE_NAME + ".dfs.core.windows.net", dbutils.secrets.get(scope = __SECRET_SCOPE, key = __SECRET_NAME_DATA_LAKE_APP_CLIENT_SECRET))
spark.conf.set("fs.azure.account.oauth2.client.endpoint." + __DATA_LAKE_NAME + ".dfs.core.windows.net", "https://login.microsoftonline.com/" + dbutils.secrets.get(scope = __SECRET_SCOPE, key = __SECRET_NAME_DATA_LAKE_APP_CLIENT_TENANT_ID) + "/oauth2/token")

# COMMAND ----------

def archiveFile(file, archivePath):
  archiveLogEntry = []
    
  if file.path.endswith('.partial') == True:
    # Do not handle files with '.partial' suffix. The suffix means that the file is not yet fully uploaded
    return archiveLogEntry   
    
  if file.size == 0:
    # Do not archive empty files
    return archiveLogEntry
    
  # 1. Create unique archive name and location
  fileName, fileExtension = os.path.splitext(file.path)
  archiveDatetime = datetime.utcnow()
  archiveFileName = archiveDatetime.strftime("%H_%M") + "_" + str(uuid.uuid4()) + fileExtension
  archiveFilePath = archivePath + "/" + archiveDatetime.strftime("%Y/%m/%d") + "/" + archiveFileName
  
  # 2. Copy file to the archive location from staging
  dbutils.fs.cp(file.path, archiveFilePath)
  print("Staged file '" + file.path +  "' archived to '" + archiveFilePath + "'")
  
  # 3. Create archive log entry
  archiveLogEntry.append({
      'ArchiveDatetimeUTC': archiveDatetime,
      'ArchiveYearUTC': int(archiveDatetime.year),
      'ArchiveMonthUTC': int(archiveDatetime.month),
      'ArchiveDayUTC': int(archiveDatetime.day),
      'ArchiveyyyyMMddUTC': int(archiveDatetime.strftime("%Y%m%d")),
      'OriginalStagingFilePath': file.path,
      'OriginalStagingFileName': file.name,
      'OriginalStagingFileSize': file.size,
      'ArchiveFilePath': archiveFilePath,
      'ArchiveFileName': archiveFileName
    })
  
  return archiveLogEntry

# COMMAND ----------

archiveLogs = []
with parallel_backend('threading', n_jobs=10):
  # 1. Copy file into archive and create in-memory archive log dataset
  result_archiveLogs = Parallel()(delayed(archiveFile)(file, __ARCHIVE_PATH) for file in dbutils.fs.ls("wasbs://" + __CONTAINER + "@" + __BLOB_STORAGE_ACCOUNT + ".blob.core.windows.net/" + __INGEST_PATH))
  [archiveLogs.extend(el) for el in result_archiveLogs]

if archiveLogs:
  # 2. Commit in-memory archive log dataset into delta table
  print('Commit archive log: ' + __ARCHIVE_LOG_PATH)
  dfArchiveLogs = spark.createDataFrame(pd.DataFrame(archiveLogs)) \
                       .selectExpr("CAST(ArchiveDatetimeUTC AS timestamp) AS ArchiveDatetimeUTC", \
                                   "CAST(ArchiveYearUTC AS int) AS ArchiveYearUTC", \
                                   "CAST(ArchiveMonthUTC AS int) AS ArchiveMonthUTC", \
                                   "CAST(ArchiveDayUTC AS int) AS ArchiveDayUTC", \
                                   "CAST(ArchiveyyyyMMddUTC AS int) AS ArchiveyyyyMMddUTC", \
                                   "CAST(OriginalStagingFilePath AS string) AS OriginalStagingFilePath", \
                                   "CAST(OriginalStagingFileName AS string) AS OriginalStagingFileName", \
                                   "CAST(OriginalStagingFileSize AS long) AS OriginalStagingFileSize", \
                                   "CAST(ArchiveFilePath AS string) AS ArchiveFilePath", \
                                   "CAST(ArchiveFileName AS string) AS ArchiveFileName", \
                                   "CAST(0 AS boolean) AS IsPurged", \
                                   "CAST(NULL AS timestamp) AS PurgeDatetimeUTC", \
                                   "CAST(0 AS boolean) AS IsIgnorable", \
                                   "CAST(NULL AS string) AS Notes")

  try:
    dfArchiveLogs.write.partitionBy("ArchiveyyyyMMddUTC") \
                       .format("delta") \
                       .mode("append") \
                       .option("mergeSchema", "true") \
                       .save(__ARCHIVE_LOG_PATH)
  except AnalysisException as err:
    if str(err).find("OriginalStagingFileSize") != -1:
      print("Preparing to fix OriginalStagingFileSize data type")
      dfFixedArchiveLogs = spark.sql('SELECT * FROM delta.`' + __ARCHIVE_LOG_PATH + '`')
      dfFixedArchiveLogs = dfFixedArchiveLogs.withColumn("OriginalStagingFileSize", col("OriginalStagingFileSize").cast("long"))
      dfFixedArchiveLogs.write.partitionBy("ArchiveyyyyMMddUTC") \
                        .format("delta") \
                        .mode("overwrite") \
                        .option("overwriteSchema", "true") \
                        .save(__ARCHIVE_LOG_PATH)
      print("Fixed OriginalStagingFileSize data type")
      print('Retrying commit archive log: ' + __ARCHIVE_LOG_PATH)
      dfArchiveLogs.write.partitionBy("ArchiveyyyyMMddUTC") \
                       .format("delta") \
                       .mode("append") \
                       .save(__ARCHIVE_LOG_PATH)
      pass
    else:
      raise
  except:
      raise

  # 3. Remove archived files
  print("Remove archived files from staging")
  with parallel_backend('threading', n_jobs=10):
    Parallel()(delayed(dbutils.fs.rm)((archiveLogRow['OriginalStagingFilePath'])) for archiveLogRow in archiveLogs)
  
  # 4. Optimize archive log
  print('Optimize archive log: ' + __ARCHIVE_LOG_PATH)
  spark.sql('OPTIMIZE delta.`' + __ARCHIVE_LOG_PATH + '`').display()

# COMMAND ----------

dbutils.notebook.exit(True)
