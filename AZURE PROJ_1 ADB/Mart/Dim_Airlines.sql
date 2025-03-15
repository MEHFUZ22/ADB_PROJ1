-- Databricks notebook source
use  hive_metastore.mart_azure;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS Dim_Airlines (
  iata_code STRING,
  icao_code STRING,
  name STRING
) USING DELTA LOCATION '/mnt/mart_datalake/Dim_Airlines'

-- COMMAND ----------

INSERT OVERWRITE Dim_Airlines
SELECT 
iata_code 
,icao_code 
,airline_name 
FROM  hive_metastore.cleansed_azurep1.airlines
