# Databricks notebook source
# MAGIC %md
# MAGIC ### Cleanse and Write data to Cleanse Tables

# COMMAND ----------

import datetime
from pyspark.sql.functions import *

# COMMAND ----------

df = spark.read.format("json").load("abfss://autoloader-source@autoloader07gen2.dfs.core.windows.net/workflow_raw/")


# COMMAND ----------

#Adding InsertedDatetime column
df_time = df.withColumn("InsertedDatetime",current_timestamp() )

# COMMAND ----------

path = "abfss://autoloader-sink@autoloader07gen2.dfs.core.windows.net/workflow_cleanse/"
df_time.write.format("delta").mode("append").save(path)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS adb_learning.default.workflow_cleanse USING DELTA LOCATION "abfss://autoloader-sink@autoloader07gen2.dfs.core.windows.net/workflow_cleanse/"

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from adb_learning.default.workflow_cleanse

# COMMAND ----------

dbutils.jobs.taskValues.set(key = "CleanseTableName" , value = "adb_learning.default.workflow_cleanse")