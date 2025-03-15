-- Databricks notebook source
use  hive_metastore.mart_azure;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS Dim_UniqueCarrier (
  code STRING,
  description STRING
) USING DELTA LOCATION '/mnt/mart_datalake/Dim_UniqueCarrier'

-- COMMAND ----------

INSERT OVERWRITE Dim_UniqueCarrier
SELECT 
code 
,description 

FROM  hive_metastore.cleansed_azurep1.Unique_Carriers
