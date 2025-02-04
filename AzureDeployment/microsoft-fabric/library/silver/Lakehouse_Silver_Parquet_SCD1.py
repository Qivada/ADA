#!/usr/bin/env python
# coding: utf-8

# ## Lakehouse_Silver_Parquet_SCD1
# 
# New notebook

# In[ ]:


__BRONZE_SCHEMA = ""
__BRONZE_TABLE = ""
__SILVER_SCHEMA = ""
__SILVER_TABLE = ""
__SILVER_TABLE_BK_COLUMNS = ""
__SILVER_TABLE_EXTRACT_COLUMNS = ""
__SILVER_TABLE_EXCLUDE_COLUMNS = ""  


# In[ ]:


# In Spark 3.1, loading and saving of timestamps from/to parquet files fails if the timestamps are before 1900-01-01 00:00:00Z, and loaded (saved) as the INT96 type. 
# In Spark 3.0, the actions don’t fail but might lead to shifting of the input timestamps due to rebasing from/to Julian to/from Proleptic Gregorian calendar. 
# To restore the behavior before Spark 3.1, you can set spark.sql.parquet.int96RebaseModeInRead or/and spark.sql.legacy.parquet.int96RebaseModeInWrite to LEGACY.
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

# Enable automatic schema evolution
spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true") 


# In[ ]:


__BRONZE_WORKSPACE_ID = "ID-OF-WORKSPACE"
__BRONZE_WORKSPACE_NAME = "NAME-OF-WORKSPACE"
__BRONZE_LAKEHOUSE = "NAME-OF-LAKEHOUSE"
__BRONZE_LAKEHOUSE_ID = "ID-OF-LAKEHOUSE"
__BRONZE_PATH = "abfss://{workspaceId}@onelake.dfs.fabric.microsoft.com/{lakehouseId}".format(workspaceId = __BRONZE_WORKSPACE_ID, lakehouseId = __BRONZE_LAKEHOUSE_ID)
__BRONZE_TABLE_FULLY_QUALIFIED_NAME = "`{workspace}`.`{lakehouse}`.`{schema}`.`{table}`".format(workspace = __BRONZE_WORKSPACE_NAME, lakehouse = __BRONZE_LAKEHOUSE,  schema = __BRONZE_SCHEMA, table = __BRONZE_TABLE)

__SILVER_WORKSPACE_ID = "ID-OF-WORKSPACE"
__SILVER_LAKEHOUSE = "NAME-OF-LAKEHOUSE"
__SILVER_LAKEHOUSE_ID = "ID-OF-LAKEHOUSE"
__SILVER_PATH = "abfss://{workspaceId}@onelake.dfs.fabric.microsoft.com/{lakehouseId}".format(workspaceId = __SILVER_WORKSPACE_ID, lakehouseId = __SILVER_LAKEHOUSE_ID)
__SILVER_TABLE_FULLY_QUALIFIED_NAME = "`{lakehouse}`.`{schema}`.`{table}`".format(lakehouse = __SILVER_LAKEHOUSE,  schema = __SILVER_SCHEMA, table = __SILVER_TABLE)
__SILVER_TABLE_SCHEMA = "{table}".format(table = __SILVER_SCHEMA)
__SILVER_TABLE_NAME = "{table}".format(table = __SILVER_TABLE)
__SILVER_LOG_TABLE_FULLY_QUALIFIED_NAME = "`{lakehouse}`.`{schema}`.`{table}_cdc_log`".format(lakehouse = __SILVER_LAKEHOUSE,  schema = __SILVER_SCHEMA, table = __SILVER_TABLE)

# Lowercase everything
__BRONZE_WORKSPACE_ID = __BRONZE_WORKSPACE_ID.lower()
__BRONZE_LAKEHOUSE = __BRONZE_LAKEHOUSE.lower()
__BRONZE_LAKEHOUSE_ID = __BRONZE_LAKEHOUSE_ID.lower()
__BRONZE_PATH = __BRONZE_PATH.lower()
__SILVER_WORKSPACE_ID = __SILVER_WORKSPACE_ID.lower()
__SILVER_LAKEHOUSE = __SILVER_LAKEHOUSE.lower()
__SILVER_LAKEHOUSE_ID = __SILVER_LAKEHOUSE_ID.lower()
__SILVER_PATH = __SILVER_PATH.lower()
__SILVER_TABLE_FULLY_QUALIFIED_NAME = __SILVER_TABLE_FULLY_QUALIFIED_NAME.lower()
__SILVER_TABLE_SCHEMA = __SILVER_TABLE_SCHEMA.lower()
__SILVER_TABLE_NAME = __SILVER_TABLE_NAME.lower()
__SILVER_LOG_TABLE_FULLY_QUALIFIED_NAME = __SILVER_LOG_TABLE_FULLY_QUALIFIED_NAME.lower()

print(f"Bronze:")
print(f"> Path: {__BRONZE_PATH}")
print(f"> Table: {__BRONZE_TABLE_FULLY_QUALIFIED_NAME}")
print(f"")
print(f"Silver:")
print(f"> Path: {__SILVER_PATH}")
print(f"> Table: {__SILVER_TABLE_FULLY_QUALIFIED_NAME}")
print(f"> Log table: {__SILVER_LOG_TABLE_FULLY_QUALIFIED_NAME}")
print(f"> Business key column(s): {__SILVER_TABLE_BK_COLUMNS}")
print(f"> Extract column(s): {__SILVER_TABLE_EXTRACT_COLUMNS}")
print(f"> Exclude column(s): {__SILVER_TABLE_EXCLUDE_COLUMNS}")
print(f"")
print(f"Create silver schema `{__SILVER_SCHEMA}` if not exists")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{__SILVER_SCHEMA}`")


# In[ ]:


import sys
from delta.tables import *
from pyspark.sql.functions import lit, col, sha2, concat_ws
from pyspark.sql.utils import AnalysisException
from datetime import datetime
import pandas as pd
from notebookutils import mssparkutils


# In[ ]:


# Get process datetimes
lastArchiveDatetimeUTC = None
try:
    # Try to read existing log
    lastArchiveDatetimeUTC = spark.sql(f"SELECT MAX(ArchiveDatetimeUTC) AS ArchiveDatetimeUTC FROM {__SILVER_LOG_TABLE_FULLY_QUALIFIED_NAME}").collect()[0][0]
    print(f"Using existing log table {__SILVER_LOG_TABLE_FULLY_QUALIFIED_NAME} with time: {str(lastArchiveDatetimeUTC)}")
except AnalysisException as ex:
    # Initiliaze log as it did not exist
    dfProcessDatetimes = spark.sql("SELECT CAST(date_sub(current_timestamp(), 5) AS timestamp) AS ArchiveDatetimeUTC")
    dfProcessDatetimes.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(__SILVER_LOG_TABLE_FULLY_QUALIFIED_NAME)
    lastArchiveDatetimeUTC = spark.sql(f"SELECT MAX(ArchiveDatetimeUTC) AS ArchiveDatetimeUTC FROM {__SILVER_LOG_TABLE_FULLY_QUALIFIED_NAME}").collect()[0][0]
    print(f"Initiliazed log table {__SILVER_LOG_TABLE_FULLY_QUALIFIED_NAME} with time: {str(lastArchiveDatetimeUTC)}")
except Exception as ex:
    print(f"Could not read log table {__SILVER_LOG_TABLE_FULLY_QUALIFIED_NAME}")
    print(ex)
    raise


# In[ ]:


def getMatchCondition(columns, note, nullSafe = True):
    includeConditionJoin = False
    conditionJoin = "AND"
    condition = ""
    
    for columnIndex, columnName in enumerate(columns):
        if includeConditionJoin == True:
            condition += " " + conditionJoin + " "
            
        if nullSafe == False:
            condition += "s." + columnName + " = t." + columnName
        else:
            condition += "s." + columnName + " <=> t." + columnName
            
        includeConditionJoin = True
    
    return condition


# In[ ]:


def silverTableExists(silverPath, schema, table):
    try:
        mssparkutils.fs.ls(f"{silverPath}/Tables/{schema}/{table}")
        return True
    except:
        pass

    return False


# In[ ]:


# Get archive log records where ArchiveDatetimeUTC is greater than lastArchiveDatetimeUTC
dfArchiveLogs = spark.sql(f" \
    SELECT * \
    FROM   {__BRONZE_TABLE_FULLY_QUALIFIED_NAME} \
    WHERE  ArchiveDatetimeUTC > CAST('{str(lastArchiveDatetimeUTC)}' AS timestamp) AND `IsPurged` = 0 AND `IsCorrupted` = 0 \
    ORDER BY ArchiveDatetimeUTC ASC \
")

__SILVER_TABLE_BK_COLUMNS = __SILVER_TABLE_BK_COLUMNS.replace('[', '').replace(']', '')
__SILVER_TABLE_BK_COLUMNS = ["`" + x.strip() + "`" for x in __SILVER_TABLE_BK_COLUMNS.split(',')]
print("Business key columns: " + ", ".join(__SILVER_TABLE_BK_COLUMNS))

__SILVER_TABLE_EXTRACT_COLUMNS = __SILVER_TABLE_EXTRACT_COLUMNS.replace('[', '').replace(']', '')
print("Extracted columns: " + __SILVER_TABLE_EXTRACT_COLUMNS)

__SILVER_TABLE_EXCLUDE_COLUMNS = __SILVER_TABLE_EXCLUDE_COLUMNS.replace('[', '').replace(']', '')
__SILVER_TABLE_EXCLUDE_COLUMNS = ["`" + x.strip() + "`" for x in __SILVER_TABLE_EXCLUDE_COLUMNS.split(',')]
print("Excluded columns: " + ", ".join(__SILVER_TABLE_EXCLUDE_COLUMNS))

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
  
    dfSource = spark.sql(f"SELECT {__SILVER_TABLE_EXTRACT_COLUMNS} FROM parquet.`{__BRONZE_PATH}/{archiveLog.ArchiveFilePath}`")

    # Replace invalid characters from column names
    renamed_column_list = list(map(lambda x: x.replace(" ", "_").replace("value.", "").replace(".", "_"), dfSource.columns))
    dfSource = dfSource.toDF(*renamed_column_list)
  
    for columnToExclude in __SILVER_TABLE_EXCLUDE_COLUMNS:
        dfSource = dfSource.drop(col(columnToExclude))
    
    dfSource = dfSource.withColumn("__HashDiff", sha2(concat_ws("||", *dfSource.columns), 256))

    if silverTableExists(__SILVER_PATH, __SILVER_TABLE_SCHEMA, __SILVER_TABLE_NAME) == False:
        print(f"> Create silver table {__SILVER_TABLE_FULLY_QUALIFIED_NAME}")    
        dfSource.withColumn('__ModifiedDatetimeUTC', lit(datetime.utcnow())) \
                  .withColumn('__ArchiveDatetimeUTC', lit(archiveLog.ArchiveDatetimeUTC)) \
                  .withColumn('__ArchiveFilePath', lit(archiveLog.ArchiveFilePath)) \
                  .withColumn('__OriginalStagingFileName', lit(archiveLog.OriginalStagingFileName)) \
                  .write.format("delta") \
                  .saveAsTable(__SILVER_TABLE_FULLY_QUALIFIED_NAME)
    else:
        print(f"> Insert/Update silver table {__SILVER_TABLE_FULLY_QUALIFIED_NAME}")    
        deltaTable = DeltaTable.forPath(spark, f"{__SILVER_PATH}/Tables/{__SILVER_TABLE_SCHEMA}/{__SILVER_TABLE_NAME}")
        deltaTable.alias("t").merge(
            dfSource.withColumn('__ModifiedDatetimeUTC', lit(datetime.utcnow())) \
                    .withColumn('__ArchiveDatetimeUTC', lit(archiveLog.ArchiveDatetimeUTC)) \
                    .withColumn('__ArchiveFilePath', lit(archiveLog.ArchiveFilePath)) \
                    .withColumn('__OriginalStagingFileName', lit(archiveLog.OriginalStagingFileName)) \
                    .alias("s"),
            getMatchCondition(__SILVER_TABLE_BK_COLUMNS, "Match business keys")
        ).whenMatchedUpdateAll(  
          condition = "s.`__HashDiff` != t.`__HashDiff`"
        ).whenNotMatchedInsertAll(
        ).execute()


# In[ ]:


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
                     .saveAsTable(__SILVER_LOG_TABLE_FULLY_QUALIFIED_NAME)

