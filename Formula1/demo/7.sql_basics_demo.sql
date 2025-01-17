-- Databricks notebook source
SHOW DATABASES;

-- COMMAND ----------

SELECT CURRENT_DATABASE()

-- COMMAND ----------

USE f1_processed;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SELECT * 
FROM drivers;

-- COMMAND ----------

DESC drivers;

-- COMMAND ----------

SELECT name, dob 
FROM drivers
WHERE nationality = "British"
AND dob >= '1990-01-01'
ORDER BY dob DESC;

-- COMMAND ----------

SELECT * FROM drivers
ORDER BY nationality ASC, 
dob DESC;

-- COMMAND ----------

SELECT name, dob, nationality 
FROM drivers
WHERE (nationality = "British"
AND dob >= '1990-01-01') 
OR nationality = 'Indian'
ORDER BY dob DESC;