-- Databricks notebook source
use  hive_metastore.mart_azure;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS Dim_Cancellation (
  code STRING,
  description STRING
) USING DELTA LOCATION '/mnt/mart_datalake/Dim_Cancellation'

-- COMMAND ----------

INSERT OVERWRITE Dim_Cancellation
SELECT 
code 
,description 
FROM hive_metastore.cleansed_azurep1.Cancellation
