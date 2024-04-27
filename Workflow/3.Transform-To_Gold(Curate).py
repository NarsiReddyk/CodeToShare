# Databricks notebook source
# MAGIC %md
# MAGIC ### Transform and Write to Curate Tables

# COMMAND ----------

CleanseTableName = dbutils.jobs.taskValues.get(taskKey= "cleanse", key = "CleanseTableName" )
# ,default = "adb_learning.default.workflow_cleanse"

# COMMAND ----------

print(CleanseTableName)

# COMMAND ----------

# tablename = "adb_learning.default.workflow_cleanse"
df = spark.table(CleanseTableName)

# COMMAND ----------

df_PriceIncrease = df.withColumn("price_Increase", df.price*0.5)

# COMMAND ----------

path = "abfss://autoloader-sink@autoloader07gen2.dfs.core.windows.net/workflow_curate/"
df_PriceIncrease.write.format("delta").mode("append").save(path)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS adb_learning.default.workflow_curate USING DELTA LOCATION "abfss://autoloader-sink@autoloader07gen2.dfs.core.windows.net/workflow_curate/"