# Databricks notebook source
# MAGIC %md
# MAGIC ### Extract data from REST API and persist in Storage account as JSON file

# COMMAND ----------

import datetime
import requests
import json
from pyspark.sql.functions import *

headers = {"content-type": "application/json"}
requestUrl = "http://www.boredapi.com/api/activity/"
response = requests.get(requestUrl, headers=headers)
data_json = response.content
decoded_data_json =data_json.decode("utf-8")[0:]

from datetime import datetime
now = datetime.now()
dt_string = now.strftime("%Y%m%d%H%M%S")

# Update the path concatenation properly
path = "abfss://autoloader-source@autoloader07gen2.dfs.core.windows.net/workflow_raw/activity_" + dt_string + ".json"

# Assuming 'data_json1' is your JSON data
dbutils.fs.put(path, str(decoded_data_json), True)