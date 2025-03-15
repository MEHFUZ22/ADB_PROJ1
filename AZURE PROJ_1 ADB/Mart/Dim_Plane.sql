-- Databricks notebook source
use  hive_metastore.mart_azure;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS DIM_PLANE (
  tailid STRING,
  type STRING,
  manufacturer STRING,
  issue_date Date,
  model STRING,
  status String,
  aircraft_type String,
  engine_type String,
  year int
) USING DELTA LOCATION '/mnt/mart_datalake/DIM_PLANE'

-- COMMAND ----------

INSERT OVERWRITE DIM_PLANE
SELECT 
tailid 
,type 
,manufacturer 
,issue_date 
,model 
,status 
,aircraft_type 
,engine_type 
,year
  FROM   hive_metastore.cleansed_azurep1.PLANE
