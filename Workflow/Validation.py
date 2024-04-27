# Databricks notebook source
# MAGIC %sql
# MAGIC select * from adb_learning.default.workflow_cleanse

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from adb_learning.default.workflow_curate

# COMMAND ----------

# MAGIC %sql
# MAGIC Truncate table adb_learning.default.workflow_cleanse

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE adb_learning.default.workflow_curate