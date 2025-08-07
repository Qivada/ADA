#!/usr/bin/env python
# coding: utf-8

# ## Lakehouse_Bronze_Archive
# 
# New notebook

# In[1]:


__BRONZE_SCHEMA = ""
__BRONZE_TABLE = ""


# In[2]:


# Lowercase everything
__BRONZE_SCHEMA = __BRONZE_SCHEMA.lower()
__BRONZE_TABLE = __BRONZE_TABLE.lower()

# Remove invalid characters
__BRONZE_SCHEMA = __BRONZE_SCHEMA.replace(".", "_")
__BRONZE_TABLE = __BRONZE_TABLE.replace(".", "_")

__BRONZE_TABLE_FULLY_QUALIFIED_NAME = "`{schema}`.`{table}`".format(schema = __BRONZE_SCHEMA, table = __BRONZE_TABLE)
print("Bronze table: {table}".format(table = __BRONZE_TABLE_FULLY_QUALIFIED_NAME))


# In[3]:


spark.sql("CREATE SCHEMA IF NOT EXISTS {schema}".format(schema = __BRONZE_SCHEMA))


# In[4]:


from notebookutils import mssparkutils
from datetime import datetime
import uuid
import pandas as pd
import os
from joblib import Parallel, delayed, parallel_backend
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import lit, col


# In[5]:


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
    archiveFileName = archiveDatetime.strftime("%Y_%m_%d-%H_%M") + "_" + str(uuid.uuid4()) + fileExtension
    archiveFilePath = archivePath + "/" + archiveDatetime.strftime("%Y/%m/%d") + "/" + archiveFileName
  
    # 2. Move file to the archive location from staging
    mssparkutils.fs.mv(src=file.path, dest=archiveFilePath, create_path=True)
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


# In[ ]:


def tableMaintenance(__TABLE_FULLY_QUALIFIED_NAME:str) -> None:
    """
    Maintenance routine for specified delta table

    Parameters
    ----------
    __TABLE_FULLY_QUALIFIED_NAME : str
        Fully qualified table name e.g. \`schema\`.\`table\`

    Returns
    -------
    None
        Function does not return anything

    Description
    -----------
    This function runs VACUUM and OPTIMIZE for specified delta table if previous VACUUM operation happened at least 14 days ago
    """

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


# In[6]:


__INGEST_PATH = f'Files/ingest/{__BRONZE_SCHEMA}/{__BRONZE_TABLE}/'
__ARCHIVE_PATH = f'Files/archive/{__BRONZE_SCHEMA}/{__BRONZE_TABLE}/'

archiveLogs = []
with parallel_backend('threading', n_jobs=10):
    # 1. Copy file into archive and create in-memory archive log dataset
    result_archiveLogs = Parallel()(delayed(archiveFile)(file, __ARCHIVE_PATH) for file in mssparkutils.fs.ls(__INGEST_PATH))
    [archiveLogs.extend(el) for el in result_archiveLogs]

if archiveLogs:
    # 2. Commit in-memory archive log dataset into delta table
    print('Commit archive log: ' + __BRONZE_TABLE)
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
                                     "CAST(0 AS boolean) AS IsCorrupted", \
                                     "CAST(NULL AS string) AS Notes")

    dfArchiveLogs.write.format("delta") \
                       .mode("append") \
                       .option("mergeSchema", "true") \
                       .saveAsTable(__BRONZE_TABLE_FULLY_QUALIFIED_NAME)

    tableMaintenance(__BRONZE_TABLE_FULLY_QUALIFIED_NAME)

