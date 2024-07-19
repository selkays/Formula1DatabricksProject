# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake cluster scoped credentials
# MAGIC 1. Set the spark config fs.azure.account.key in the cluster
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from curcuits.csv file
# MAGIC

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formul1datalake.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formul1datalake.dfs.core.windows.net/circuits.csv"))