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


__VARIABLE_LIBRARY = notebookutils.variableLibrary.getLibrary("Lakehouse_Silver_Variable_Library")

__BRONZE_WORKSPACE_ID = __VARIABLE_LIBRARY.BRONZE_WORKSPACE_ID
__BRONZE_WORKSPACE_NAME = __VARIABLE_LIBRARY.BRONZE_WORKSPACE_NAME
__BRONZE_LAKEHOUSE = __VARIABLE_LIBRARY.BRONZE_LAKEHOUSE_NAME
__BRONZE_LAKEHOUSE_ID = __VARIABLE_LIBRARY.BRONZE_LAKEHOUSE_ID
__BRONZE_PATH = "abfss://{workspaceId}@onelake.dfs.fabric.microsoft.com/{lakehouseId}".format(workspaceId = __BRONZE_WORKSPACE_ID, lakehouseId = __BRONZE_LAKEHOUSE_ID)
__BRONZE_TABLE_FULLY_QUALIFIED_NAME = "`{workspace}`.`{lakehouse}`.`{schema}`.`{table}`".format(workspace = __BRONZE_WORKSPACE_NAME, lakehouse = __BRONZE_LAKEHOUSE,  schema = __BRONZE_SCHEMA, table = __BRONZE_TABLE)

__SILVER_WORKSPACE_ID = __VARIABLE_LIBRARY.SILVER_WORKSPACE_ID
__SILVER_LAKEHOUSE = __VARIABLE_LIBRARY.SILVER_LAKEHOUSE_NAME
__SILVER_LAKEHOUSE_ID = __VARIABLE_LIBRARY.SILVER_LAKEHOUSE_ID
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
from pyspark.sql.functions import lit, col, sha1, sha2, concat_ws
from pyspark.sql.utils import AnalysisException
from datetime import datetime
import pandas as pd
from notebookutils import mssparkutils


# In[ ]:


# Get process datetimes
lastArchiveDatetimeUTC = None
try:
    # Try to read existing log
    lastArchiveDatetimeUTC = spark.sql("""
        SELECT  MAX(ArchiveDatetimeUTC) AS ArchiveDatetimeUTC 
        FROM    {LOG_TABLE}
        WHERE   BronzeSchema = '{SCHEMA}' AND
                BronzeTable = '{TABLE}'
        GROUP BY BronzeSchema, BronzeTable
        """.format(LOG_TABLE = __SILVER_LOG_TABLE_FULLY_QUALIFIED_NAME, SCHEMA = __BRONZE_SCHEMA, TABLE = __BRONZE_TABLE)).collect()[0][0]
        
    print(f"Using existing log table {__SILVER_LOG_TABLE_FULLY_QUALIFIED_NAME} with time: {str(lastArchiveDatetimeUTC)}")
except AnalysisException as ex:
    # Initiliaze log as it did not exist
    dfProcessDatetimes = spark.sql("SELECT CAST(date_sub(current_timestamp(), 90) AS timestamp) AS ArchiveDatetimeUTC, '{SCHEMA}' AS BronzeSchema, '{TABLE}' AS BronzeTable".format(SCHEMA = __BRONZE_SCHEMA, TABLE = __BRONZE_TABLE))
    dfProcessDatetimes.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(__SILVER_LOG_TABLE_FULLY_QUALIFIED_NAME)
    lastArchiveDatetimeUTC = spark.sql("""
        SELECT  MAX(ArchiveDatetimeUTC) AS ArchiveDatetimeUTC 
        FROM    {LOG_TABLE}
        WHERE   BronzeSchema = '{SCHEMA}' AND
                BronzeTable = '{TABLE}'
        GROUP BY BronzeSchema, BronzeTable
        """.format(LOG_TABLE = __SILVER_LOG_TABLE_FULLY_QUALIFIED_NAME, SCHEMA = __BRONZE_SCHEMA, TABLE = __BRONZE_TABLE)).collect()[0][0]

    print(f"Initiliazed log table {__SILVER_LOG_TABLE_FULLY_QUALIFIED_NAME} with time: {str(lastArchiveDatetimeUTC)} for bronze source `{str(__BRONZE_SCHEMA)}`.`{str(__BRONZE_TABLE)}`")
except IndexError as ex:
    # Initiliaze log table for another BronzeSchema/BronzeTable combination
    dfProcessDatetimes = spark.sql("SELECT CAST(date_sub(current_timestamp(), 90) AS timestamp) AS ArchiveDatetimeUTC, '{SCHEMA}' AS BronzeSchema, '{TABLE}' AS BronzeTable".format(SCHEMA = __BRONZE_SCHEMA, TABLE = __BRONZE_TABLE))
    dfProcessDatetimes.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(__SILVER_LOG_TABLE_FULLY_QUALIFIED_NAME)
    lastArchiveDatetimeUTC = spark.sql("""
        SELECT  MAX(ArchiveDatetimeUTC) AS ArchiveDatetimeUTC 
        FROM    {LOG_TABLE}
        WHERE   BronzeSchema = '{SCHEMA}' AND
                BronzeTable = '{TABLE}'
        GROUP BY BronzeSchema, BronzeTable
        """.format(LOG_TABLE = __SILVER_LOG_TABLE_FULLY_QUALIFIED_NAME, SCHEMA = __BRONZE_SCHEMA, TABLE = __BRONZE_TABLE)).collect()[0][0]

    print(f"Initiliazed log table {__SILVER_LOG_TABLE_FULLY_QUALIFIED_NAME} with time: {str(lastArchiveDatetimeUTC)} for bronze source `{str(__BRONZE_SCHEMA)}`.`{str(__BRONZE_TABLE)}`")
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


def tableMaintenance(__TABLE_FULLY_QUALIFIED_NAME):
    spark.sql("""
        DESCRIBE HISTORY {table}
    """.format(table=__TABLE_FULLY_QUALIFIED_NAME)).createOrReplaceTempView("tableHistory")

    try:
        LastMaintenanceSinceDays = spark.sql("""
            SELECT  IFNULL(DATEDIFF(current_timestamp, T1.lastMaintenceTimestamp), -1) AS LastMaintenanceSinceDays
            FROM    (
                        SELECT  MAX(timestamp) AS lastMaintenceTimestamp 
                        FROM    tableHistory
                        WHERE   operation in ('VACUUM END', 'VACUUM START')
                    ) AS T1 
        """).collect()[0][0]

        if LastMaintenanceSinceDays < 0:
            print("Table {table} requires initial maintenance".format(table=__TABLE_FULLY_QUALIFIED_NAME))
        
            # VACUUM and OPTIMIZE archive table
            spark.sql("VACUUM {TABLE}".format(TABLE=__TABLE_FULLY_QUALIFIED_NAME))
            spark.sql("OPTIMIZE {TABLE}".format(TABLE=__TABLE_FULLY_QUALIFIED_NAME))
        elif LastMaintenanceSinceDays >= 14:
            print("Table {table} does requires maintenance. Last maintenance occured {daysAgo} ago".format(table=__TABLE_FULLY_QUALIFIED_NAME, daysAgo=LastMaintenanceSinceDays))
            
            # VACUUM and OPTIMIZE archive table
            spark.sql("VACUUM {TABLE}".format(TABLE=__TABLE_FULLY_QUALIFIED_NAME))
            spark.sql("OPTIMIZE {TABLE}".format(TABLE=__TABLE_FULLY_QUALIFIED_NAME))
        else:
            print("Table {table} does not require maintenance. Last maintenance occured {daysAgo} ago".format(table=__TABLE_FULLY_QUALIFIED_NAME, daysAgo=LastMaintenanceSinceDays))
    except:
        print("Unable to determine table maintenance requirement")
        pass


# In[ ]:


# Get archive log records where ArchiveDatetimeUTC is greater than lastArchiveDatetimeUTC
dfArchiveLogs = spark.sql(f" \
    SELECT * \
    FROM   {__BRONZE_TABLE_FULLY_QUALIFIED_NAME} \
    WHERE  ArchiveDatetimeUTC > CAST('{str(lastArchiveDatetimeUTC)}' AS timestamp) AND `IsPurged` = 0 AND `IsCorrupted` = 0 \
    ORDER BY ArchiveDatetimeUTC ASC \
")

column_rename_rules = {
    ' ': '_', 
    ',': '_', 
    ';': '_', 
    '{': '_', 
    '}': '_', 
    '(': '_', 
    ')': '_', 
    '\n': '_', 
    '\t': '_', 
    '=': '_', 
    '.': '_',
    '[': '',
    ']': ''
}

__SILVER_TABLE_EXTRACT_COLUMNS = __SILVER_TABLE_EXTRACT_COLUMNS.replace('[', '').replace(']', '')
print("Extracted columns: " + __SILVER_TABLE_EXTRACT_COLUMNS)

__SILVER_TABLE_BK_COLUMNS = ["`" + x.strip().translate(str.maketrans(column_rename_rules)) + "`" for x in __SILVER_TABLE_BK_COLUMNS.split(',')]
print("Business key columns: " + ", ".join(__SILVER_TABLE_BK_COLUMNS))

__SILVER_TABLE_EXCLUDE_COLUMNS = ["`" + x.strip().translate(str.maketrans(column_rename_rules)) + "`" for x in __SILVER_TABLE_EXCLUDE_COLUMNS.split(',')]
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
      'ArchiveFileName': archiveLog.ArchiveFileName,
      'BronzeSchema': __BRONZE_SCHEMA,
      'BronzeTable': __BRONZE_TABLE
    })
  
    dfSource = spark.sql(f"SELECT {__SILVER_TABLE_EXTRACT_COLUMNS} FROM parquet.`{__BRONZE_PATH}/{archiveLog.ArchiveFilePath}`")

    # Replace invalid characters from column names
    renamed_column_list = list(map(lambda x: x.replace(" ", "_").replace("value.", "").replace(".", "_"), dfSource.columns))
    dfSource = dfSource.toDF(*renamed_column_list)
  
    for columnToExclude in __SILVER_TABLE_EXCLUDE_COLUMNS:
        dfSource = dfSource.drop(col(columnToExclude))
    
    dfSource = dfSource.withColumn("__HashDiff", sha2(concat_ws("||", *dfSource.columns), 256)) \
                       .withColumn("__PrimaryKey", sha1(concat_ws("||", *__SILVER_TABLE_BK_COLUMNS)))

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
                                   "CAST(ArchiveFileName AS string) AS ArchiveFileName", \
                                   "CAST(BronzeSchema AS string) AS BronzeSchema", \
                                   "CAST(BronzeTable AS string) AS BronzeTable")
    dfProcessLogs.write.format("delta") \
                     .mode("append") \
                     .option("mergeSchema", "true") \
                     .saveAsTable(__SILVER_LOG_TABLE_FULLY_QUALIFIED_NAME)

    tableMaintenance(__SILVER_LOG_TABLE_FULLY_QUALIFIED_NAME)

