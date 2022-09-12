# Databricks notebook source
# DBTITLE 1,Information
# MAGIC %md
# MAGIC Populate databricks database table with slowly changing dimension type 1 logic from Azure SQL DB with SQL query.
# MAGIC 
# MAGIC Required additional libraries:
# MAGIC - None
# MAGIC 
# MAGIC Example call:
# MAGIC ```
# MAGIC returnFlag = dbutils.notebook.run(
# MAGIC   path = "/DataLake/__Library/FromAzureSqlToDatabricks/FromAzureSqlToDatabricksScd1", 
# MAGIC   timeout_seconds = 0, 
# MAGIC   arguments = {
# MAGIC     "JDBC_CONNECTION_STRING": "SQL-JDBC-connection-string",
# MAGIC     "SOURCE_SQL": "SELECT * FROM [dw].[D_Customer]",
# MAGIC     "TARGET_DATABASE": "dw",
# MAGIC     "TARGET_TABLE": "d_customer",
# MAGIC     "TARGET_TABLE_BK_COLUMNS": "CustomerID",
# MAGIC     "TARGET_PATH": "/analytics/datahub/dw/d_customer/data",
# MAGIC     "EXTRACT_COLUMNS": "*",
# MAGIC     "EXCLUDE_COLUMNS": "__HashDiff, __InsertAuditKEY, __UpdateAuditKEY"    
# MAGIC   }
# MAGIC )
# MAGIC ```

# COMMAND ----------

# Parameters
try:
  __SECRET_NAME_SQL_JDBC_CONNECTION_STRING = "SQL-JDBC-connection-string"
  try:
    __SECRET_NAME_SQL_JDBC_CONNECTION_STRING = dbutils.widgets.get("JDBC_CONNECTION_STRING")
  except:
    print("Using default JDBC connection string: " + __SECRET_NAME_SQL_JDBC_CONNECTION_STRING)
    
  # Target database e.g. SELECT * FROM [dw].[D_Customer]
  __SOURCE_SQL = dbutils.widgets.get("SOURCE_SQL")
  __SOURCE_SQL = "(" + __SOURCE_SQL + ") SRC"
  
  # Target database e.g. CRM
  __TARGET_DATABASE = dbutils.widgets.get("TARGET_DATABASE")
  
  # Target table e.g. Account
  __TARGET_TABLE = dbutils.widgets.get("TARGET_TABLE")
  
  # Target table business key columns e.g. CustomerID
  __TARGET_TABLE_BK_COLUMNS = dbutils.widgets.get("TARGET_TABLE_BK_COLUMNS")
  
  # Target path e.g. analytics/datalake/crm/account/data
  __TARGET_PATH = dbutils.widgets.get("TARGET_PATH")
 
  # Columns to extract e.g. * or AddressID, AddressLine1, AddressLine2, City, StateProvince, CountryRegion, PostalCode, rowguid, ModifiedDate
  __EXTRACT_COLUMNS = dbutils.widgets.get("EXTRACT_COLUMNS")
  
  # Columns to eclude from final data set e.g. PasswordHash, PasswordSalt
  __EXCLUDE_COLUMNS = ""  
  try:
    __EXCLUDE_COLUMNS = dbutils.widgets.get("EXCLUDE_COLUMNS")
  except:
    print('No columns to exclude')
    
  # Partition by columns e.g. __YearPartition, __MonthPartition
  __PARTITION_BY_COLUMNS = ""  
  try:
    __PARTITION_BY_COLUMNS = dbutils.widgets.get("PARTITION_BY_COLUMNS")
  except:
    print('No partition by columns')  
    
except:
  raise Exception("Required parameter(s) missing")

# COMMAND ----------

# Import
from delta.tables import *
from pyspark.sql.functions import lit, col, sha2, concat_ws
from datetime import datetime

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

__TARGET_PATH = "abfss://datahub@" + __DATA_LAKE_NAME + ".dfs.core.windows.net/" + __TARGET_PATH

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

# SQL database authentication
__SQL_JDBC = dbutils.secrets.get(scope = __SECRET_SCOPE, key = __SECRET_NAME_SQL_JDBC_CONNECTION_STRING)

# COMMAND ----------

def getMatchCondition(columns, note, nullSafe = False):
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

# COMMAND ----------

__TARGET_TABLE_BK_COLUMNS = __TARGET_TABLE_BK_COLUMNS.replace('[', '').replace(']', '')
__TARGET_TABLE_BK_COLUMNS = ["`" + x.strip() + "`" for x in __TARGET_TABLE_BK_COLUMNS.split(',')]
print("Business key columns: " + ", ".join(__TARGET_TABLE_BK_COLUMNS))

__EXTRACT_COLUMNS = __EXTRACT_COLUMNS.replace('[', '').replace(']', '')
print("Extracted columns: " + __EXTRACT_COLUMNS)

__EXCLUDE_COLUMNS = __EXCLUDE_COLUMNS.replace('[', '').replace(']', '')
__EXCLUDE_COLUMNS = ["`" + x.strip() + "`" for x in __EXCLUDE_COLUMNS.split(',')]
print("Excluded columns: " + ", ".join(__EXCLUDE_COLUMNS))

if __PARTITION_BY_COLUMNS != '':
  __PARTITION_BY_COLUMNS = __PARTITION_BY_COLUMNS.replace('[', '').replace(']', '')
  __PARTITION_BY_COLUMNS = ["`" + x.strip() + "`" for x in __PARTITION_BY_COLUMNS.split(',')]
  print("Partition by columns: " + ", ".join(__PARTITION_BY_COLUMNS))
else:
  __PARTITION_BY_COLUMNS = None

print("Processing SQL query: " + __SOURCE_SQL)
dfSource = spark.read.jdbc(url=__SQL_JDBC, table=__SOURCE_SQL)

for columnToExclude in __EXCLUDE_COLUMNS:
  dfSource = dfSource.drop(col(columnToExclude))

dfSource = dfSource.withColumn("__HashDiff", sha2(concat_ws("||", *dfSource.columns), 256))

if spark.catalog._jcatalog.tableExists(__TARGET_DATABASE + "." + __TARGET_TABLE) == False:
  print("Initial table creation")
  spark.sql("CREATE DATABASE IF NOT EXISTS " + __TARGET_DATABASE)

  if __PARTITION_BY_COLUMNS is None:
    dfSource.withColumn('__ModifiedDatetimeUTC', lit(datetime.utcnow())) \
            .write.format("delta") \
            .option("path", __TARGET_PATH) \
            .saveAsTable(__TARGET_DATABASE + "." + __TARGET_TABLE)
  else:
    dfSource.withColumn('__ModifiedDatetimeUTC', lit(datetime.utcnow())) \
            .write.format("delta") \
            .option("path", __TARGET_PATH) \
            .partitionBy(__PARTITION_BY_COLUMNS) \
            .saveAsTable(__TARGET_DATABASE + "." + __TARGET_TABLE)
else:
  print("Insert & update")
  deltaTable = DeltaTable.forPath(spark, __TARGET_PATH)
  deltaTable.alias("t").merge(
      dfSource.withColumn('__ModifiedDatetimeUTC', lit(datetime.utcnow())) \
              .alias("s"),
      getMatchCondition(__TARGET_TABLE_BK_COLUMNS, "Match business keys")
  ).whenMatchedUpdateAll(  
    condition = "s.`__HashDiff` != t.`__HashDiff`"
  ).whenNotMatchedInsertAll(
  ).execute()
  
print('Optimize delta: ' + __TARGET_PATH)
spark.sql('OPTIMIZE delta.`' + __TARGET_PATH + '`').display()

# COMMAND ----------

# Return success
dbutils.notebook.exit(True)
