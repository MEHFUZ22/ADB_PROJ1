# Databricks notebook source
# MAGIC
# MAGIC %run "/Workspace/AZURE PROJ_1 ADB/Utilities/Utilities"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
checkpoint_loc="/mnt/cleansed_datalake/Airport"

# COMMAND ----------

df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation",checkpoint_loc)
    .load("/mnt/raw_datalake/Airport/")
    .withColumn("Date_Part",current_timestamp())
)

# COMMAND ----------

df_base = df.selectExpr(
    "Code as code",
    "split(Description,',')[0] as city",
    "split(split(Description,',')[1],':')[0] as country",
    "split(split(Description,',')[1],':')[1] as airport",
    "to_date(Date_Part,'yyyy-MM-dd') as Date_Part"
)
df_base.writeStream.trigger(availableNow=True).format("delta").option(
    "checkpointLocation", checkpoint_loc
).start("/mnt/cleansed_datalake/Airport")

# COMMAND ----------


f_delta_cleansed_load("Airport", "/mnt/cleansed_datalake/Airport","cleansed_azurep1")
