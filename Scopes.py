# Databricks notebook source
#list Scopes
dbutils.secrets.listScopes()

# COMMAND ----------

#List secrets in a scope
dbutils.secrets.list("trail-kv-01")

# COMMAND ----------

#To see value of secret
dbutils.secrets.get(scope = "trail-kv-01" , key = "devstoragekey")

# COMMAND ----------

#To use in Code or workflow
value = dbutils.secrets.get(scope = "trail-kv-01" , key = "devstoragekey")

# COMMAND ----------

#if you have sufficient permissions
password = dbutils.secrets.get(scope = "trail-kv-01" , key = "devstoragekey")

# COMMAND ----------

for passwordValue in password:
    print(passwordValue, end=",")
