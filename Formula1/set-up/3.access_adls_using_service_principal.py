# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using Service Principal
# MAGIC 1. Register Azure AD Application / Service Principal
# MAGIC 2. Generate a secret/password for the application
# MAGIC 3. Set spark config with app/client ID, Directory/Tenant ID and secret
# MAGIC 4. Assign role 'Storage Blob Data Contributor' to the Data Lake.
# MAGIC

# COMMAND ----------

client_id = dbutils.secrets.get(scope = "formula1-scope", key = "formula1-app-client-id")
tenant_id = dbutils.secrets.get(scope = "formula1-scope", key = "formula1-app-tenant-id")
client_secret = dbutils.secrets.get(scope = "formula1-scope", key = "formula1-app-client-secret")

# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type.formul1datalake.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formul1datalake.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formul1datalake.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formul1datalake.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formul1datalake.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formul1datalake.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formul1datalake.dfs.core.windows.net/circuits.csv"))