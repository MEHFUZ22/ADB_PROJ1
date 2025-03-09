# Databricks notebook source
# MAGIC
# MAGIC %run "/Workspace/AZURE PROJ_1 ADB/Utilities/Utilities"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
checkpoint_loc="/mnt/cleansed_datalake/UNIQUE_CARRIERS"

# COMMAND ----------

df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", checkpoint_loc)
    .load("/mnt/raw_datalake/UNIQUE_CARRIERS.parquet/")
)

# COMMAND ----------



# COMMAND ----------

df_base = df.selectExpr(
    "replace(Code,'\"','') as code",
    "replace(Description,'\"','') as description",
    "to_date(Date_Part,'yyyy-MM-dd') as Date_Part"
)
df_base.writeStream.trigger(availableNow=True).format("delta").option(
    "checkpointLocation", "/dbfs/FileStore/tables/checkpointLocation/UNIQUE_CARRIERS"
).start("/mnt/cleansed_datalake/UNIQUE_CARRIERS")

# COMMAND ----------

f_delta_cleansed_load('UNIQUE_CARRIERS','/mnt/cleansed_datalake/UNIQUE_CARRIERS','cleansed_azurep1')
