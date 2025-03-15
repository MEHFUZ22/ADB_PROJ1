# Databricks notebook source
# MAGIC
# MAGIC %run "/Workspace/CODES/ADB_PROJ ONE/AZURE PROJ_1 ADB/Utilities/Utilities"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
checkpoint_loc="/mnt/cleansed_datalake/Cancellation"

# COMMAND ----------

df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", checkpoint_loc)
    .load("/mnt/raw_datalake/Cancellation.parquet/")
)

# COMMAND ----------

df_base = df.selectExpr(
    "replace(Code,'\"','') as code",
    "replace(Description,'\"','') as description",
    "to_date(Date_Part,'yyyy-MM-dd') as Date_Part"
)
df_base.writeStream.trigger(availableNow=True).format("delta").option(
    "checkpointLocation", checkpoint_loc
).start("/mnt/cleansed_datalake/Cancellation")

# COMMAND ----------

f_delta_cleansed_load('Cancellation','/mnt/cleansed_datalake/Cancellation','cleansed_azurep1')
