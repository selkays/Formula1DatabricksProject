# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using SAS token
# MAGIC 1. Set the spark config for SAS token
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from curcuits.csv file
# MAGIC

# COMMAND ----------

formula1dl_demo_sas_token = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dl-demo-sas-token')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formul1datalake.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formul1datalake.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formul1datalake.dfs.core.windows.net", formula1dl_demo_sas_token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formul1datalake.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formul1datalake.dfs.core.windows.net/circuits.csv"))