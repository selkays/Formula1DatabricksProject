-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/formul1datalake/processed"

-- COMMAND ----------

DESC DATABASE f1_processed;