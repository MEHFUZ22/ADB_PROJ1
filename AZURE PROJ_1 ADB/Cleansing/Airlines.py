# Databricks notebook source
# MAGIC
# MAGIC %run "/Workspace/AZURE PROJ_1 ADB/Utilities/Utilities"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,explode
checkpoint_loc="/mnt/cleansed_datalake/Airlines"

# COMMAND ----------

df=spark.read.json("/mnt/raw_datalake/airlines/")
df1=df.select(explode("data"),"Date_Part")
df_final=df1.select("col.*","Date_Part")

# COMMAND ----------

df_final.write.format("delta").mode("overwrite").save("/mnt/cleansed_datalake/Airlines")

# COMMAND ----------



f_delta_cleansed_load("Airlines", "/mnt/cleansed_datalake/Airlines","cleansed_azurep1")
