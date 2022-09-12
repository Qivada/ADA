# Databricks notebook source
# DBTITLE 1,Information
# MAGIC %md
# MAGIC Populate databricks database table with fact logic from archive CSV files.
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
  
  # Target database e.g. CRM
  __TARGET_DATABASE = dbutils.widgets.get("TARGET_DATABASE")
  
  # Target table e.g. Account
  __TARGET_TABLE = dbutils.widgets.get("TARGET_TABLE")
  
  # Target table business key columns e.g. CustomerID
  __TARGET_TABLE_BK_COLUMNS = dbutils.widgets.get("TARGET_TABLE_BK_COLUMNS")
  
  # Target path e.g. analytics/datalake/crm/account/data
  __TARGET_PATH = dbutils.widgets.get("TARGET_PATH")
  
  # Target process datetime log path e.g. analytics/datalake/crm/account/log/
  __TARGET_LOG_PATH = dbutils.widgets.get("TARGET_LOG_PATH")
  
  # Columns to extract e.g. * or AddressID, AddressLine1, AddressLine2, City, StateProvince, CountryRegion, PostalCode, rowguid, ModifiedDate
  __EXTRACT_COLUMNS = dbutils.widgets.get("EXTRACT_COLUMNS")
  
  # Columns to eclude from final data set e.g. PasswordHash, PasswordSalt
  __EXCLUDE_COLUMNS = ""  
  try:
    __EXCLUDE_COLUMNS = dbutils.widgets.get("EXCLUDE_COLUMNS")
  except:
    print('No columns to exclude')
     
  # Partition by columns pre SQL e.g. year(`transactiondate`) as __YearPartition, month(`transactiondate`) as __MonthPartition, 
  __PARTITION_BY_COLUMNS_PRE_SQL = ""  
  try:
    __PARTITION_BY_COLUMNS_PRE_SQL = dbutils.widgets.get("PARTITION_BY_COLUMNS_PRE_SQL")
  except:
    print('No partition by column pre SQL')  

  # Partition by columns e.g. __YearPartition, __MonthPartition
  __PARTITION_BY_COLUMNS = ""  
  try:
    __PARTITION_BY_COLUMNS = dbutils.widgets.get("PARTITION_BY_COLUMNS")
  except:
    print('No partition by columns')  
    
  # Default encoding
  __ENCODING = "ISO-8859-1"
  try:
    __ENCODING = dbutils.widgets.get("ENCODING")
  except:
    print('Using default encoding: ' + __ENCODING)
    
  # Default delimiter
  __DELIMITER = ","
  try:
    __DELIMITER = dbutils.widgets.get("DELIMITER")
  except:
    print('Using default delimiter: ' + __DELIMITER)
    
except:
  raise Exception("Required parameter(s) missing")

# COMMAND ----------

# Import
import sys
from delta.tables import *
from pyspark.sql.functions import lit, col, sha2, concat_ws
from pyspark.sql.types import StringType
from pyspark.sql.utils import AnalysisException
from datetime import datetime
import uuid
import pandas as pd

# Enable automatic schema evolution and optimization
spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true") 
spark.sql("SET spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = true") 
spark.sql("SET spark.databricks.delta.properties.defaults.autoOptimize.autoCompact = true") 
spark.sql("SET spark.databricks.delta.merge.repartitionBeforeWrite.enabled = true")

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

# Delta optimization
# https://docs.databricks.com/delta/optimizations/auto-optimize.html#how-auto-optimize-works
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", True)
spark.conf.set("spark.databricks.delta.autoCompact.enabled", False)         # Not to be enabled because of regular OPTIMIZE calls on table
spark.conf.set("spark.databricks.delta.autoCompact.maxFileSize", 134217728) # 128 MB

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

def getMatchCondition(columns, note, sourceAlias = "s", targetAlias = "t", nullSafe = True):
    includeConditionJoin = False
    conditionJoin = "AND"
    condition = ""
    
    for columnIndex, columnName in enumerate(columns):
        if includeConditionJoin == True:
            condition += " " + conditionJoin + " "
            
        if nullSafe == False:
            condition += sourceAlias + "." + columnName + " = " + targetAlias + "." + columnName
        else:
            condition += sourceAlias + "." + columnName + " <=> " + targetAlias + "." + columnName
            
        includeConditionJoin = True
        
    return condition

# COMMAND ----------

def getPartitionCondition(dfSource, columns, note, targetAlias = "t", nullSafe = True):
    condition = ""
    
    if columns is None:
        return condition
    
    for partitionColumn in columns:
        sPartitionValues = ""
        
        dfPartitionValues = dfSource.select(partitionColumn).distinct()
        
        partitionColumnStripped = partitionColumn.lstrip('`').rstrip('`')
        
        lPartitionValues = list(dfPartitionValues.select(partitionColumn).toPandas()[partitionColumnStripped])
        
        sPartitionValues = ",".join(f"'{pv}'" for pv in lPartitionValues if not str(pv).isnumeric())
        
        if sPartitionValues == "":
            sPartitionValues = ",".join(str(pv) for pv in lPartitionValues)
            
        condition = condition + f" AND {targetAlias}.{partitionColumn} IN ({sPartitionValues})"
    
    print("Partition optimization:" + condition)
    return condition

# COMMAND ----------

def getColumnsWithAlias(columns, alias):
  includeConditionJoin = False
  conditionJoin = ", "
  condition = ""
  
  for columnIndex, columnName in enumerate(columns):
    if includeConditionJoin == True:
      condition += conditionJoin
      
    condition += alias + "." + columnName
    includeConditionJoin = True
    
  return condition

# COMMAND ----------

# Get archive log records where ArchiveDatetimeUTC is greater than lastArchiveDatetimeUTC
dfArchiveLogs = spark.sql(" \
  SELECT * \
  FROM   delta.`" + __ARCHIVE_LOG_PATH + "` \
  WHERE  ArchiveDatetimeUTC > CAST('" + str(lastArchiveDatetimeUTC) + "' AS timestamp) \
  ORDER BY ArchiveDatetimeUTC ASC \
")

__TARGET_TABLE_BK_COLUMNS = __TARGET_TABLE_BK_COLUMNS.replace('[', '').replace(']', '')
__TARGET_TABLE_BK_COLUMNS = ["`" + x.strip() + "`" for x in __TARGET_TABLE_BK_COLUMNS.split(',')]
print("Business key columns: " + ", ".join(__TARGET_TABLE_BK_COLUMNS))

__TARGET_TABLE_BK_COLUMNS_FILTER = " IS NOT NULL AND ".join(__TARGET_TABLE_BK_COLUMNS) + " IS NOT NULL"
print("Business key filter: "+ __TARGET_TABLE_BK_COLUMNS_FILTER)

__EXTRACT_COLUMNS = __EXTRACT_COLUMNS.replace('[', '').replace(']', '')
print("Extracted columns: " + __EXTRACT_COLUMNS)

__EXCLUDE_COLUMNS = __EXCLUDE_COLUMNS.replace('[', '').replace(']', '')
__EXCLUDE_COLUMNS = ["`" + x.strip() + "`" for x in __EXCLUDE_COLUMNS.split(',')]
print("Excluded columns: " + ", ".join(__EXCLUDE_COLUMNS))

if __PARTITION_BY_COLUMNS_PRE_SQL != "":
  print("Partition by columns pre SQL: " + __PARTITION_BY_COLUMNS_PRE_SQL)

if __PARTITION_BY_COLUMNS != '':
  __PARTITION_BY_COLUMNS = __PARTITION_BY_COLUMNS.replace('[', '').replace(']', '')
  __PARTITION_BY_COLUMNS = ["`" + x.strip() + "`" for x in __PARTITION_BY_COLUMNS.split(',')]
  print("Partition by columns: " + ", ".join(__PARTITION_BY_COLUMNS))
else:
  __PARTITION_BY_COLUMNS = None

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
  
  dfSource = spark.read.option("header", True).option("encoding", __ENCODING).option("delimiter", __DELIMITER).csv(archiveLog.ArchiveFilePath).where(__TARGET_TABLE_BK_COLUMNS_FILTER)
  
  for columnToExclude in __EXCLUDE_COLUMNS:
    dfSource = dfSource.drop(col(columnToExclude))
    
  dfSource = dfSource.withColumn("__HashDiff", sha2(concat_ws("||", *dfSource.columns), 256))
  datetimeUtcNow = datetime.utcnow()
  
  # Remove empty spaces from column names as those are not supported
  renamed_column_list = list(map(lambda x: x.replace(" ", "_"), dfSource.columns))
  dfSource = dfSource.toDF(*renamed_column_list)
  
  if spark.catalog._jcatalog.tableExists(__TARGET_DATABASE + "." + __TARGET_TABLE) == False:
    print("Initial table creation")
    spark.sql("CREATE DATABASE IF NOT EXISTS " + __TARGET_DATABASE)    
    
    if __PARTITION_BY_COLUMNS is None:
      # Initial table creation without partition
      dfSource.withColumn('__ModifiedDatetimeUTC', lit(datetimeUtcNow)) \
              .withColumn('__ArchiveDatetimeUTC', lit(archiveLog.ArchiveDatetimeUTC)) \
              .withColumn('__ArchiveFilePath', lit(archiveLog.ArchiveFilePath)) \
              .withColumn('__OriginalStagingFileName', lit(archiveLog.OriginalStagingFileName)) \
              .write.format("delta") \
              .option("path", __TARGET_PATH) \
              .saveAsTable(__TARGET_DATABASE + "." + __TARGET_TABLE)
    else:
      # Initial table creation with partition
      dfSource.withColumn('__ModifiedDatetimeUTC', lit(datetimeUtcNow)) \
              .withColumn('__ArchiveDatetimeUTC', lit(archiveLog.ArchiveDatetimeUTC)) \
              .withColumn('__ArchiveFilePath', lit(archiveLog.ArchiveFilePath)) \
              .withColumn('__OriginalStagingFileName', lit(archiveLog.OriginalStagingFileName)) \
              .write.format("delta") \
              .option("path", __TARGET_PATH) \
              .partitionBy(__PARTITION_BY_COLUMNS) \
              .saveAsTable(__TARGET_DATABASE + "." + __TARGET_TABLE)
  else:
    print("Insert & update")
    # Insert & update to existing table
    deltaTable = DeltaTable.forPath(spark, __TARGET_PATH)
    deltaTable.alias("t").merge(
        dfSource.withColumn('__ModifiedDatetimeUTC', lit(datetimeUtcNow)) \
                .withColumn('__ArchiveDatetimeUTC', lit(archiveLog.ArchiveDatetimeUTC)) \
                .withColumn('__ArchiveFilePath', lit(archiveLog.ArchiveFilePath)) \
                .withColumn('__OriginalStagingFileName', lit(archiveLog.OriginalStagingFileName)) \
                .alias("s"),
        getMatchCondition(__TARGET_TABLE_BK_COLUMNS, "Match business keys") + getPartitionCondition(dfSource, __PARTITION_BY_COLUMNS, "Match partition keys")
    ).whenMatchedUpdateAll(  
      condition = "s.`__HashDiff` != t.`__HashDiff`"
    ).whenNotMatchedInsertAll(
    ).execute()
    
    try:
      spark.catalog.dropTempView(dfSourceTempViewName)
    except:
      pass

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
  
  print('Optimize data delta: ' + __TARGET_PATH)
  spark.sql('OPTIMIZE delta.`' + __TARGET_PATH + '`').display()
  
  print('Optimize log delta: ' + __TARGET_LOG_PATH)
  spark.sql('OPTIMIZE delta.`' + __TARGET_LOG_PATH + '`').display()

# COMMAND ----------

# Return success
dbutils.notebook.exit(True)
