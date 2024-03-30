# Databricks notebook source
# MAGIC %md
# MAGIC ### Variables

# COMMAND ----------

input_file = "abfss://autoloader-source@autoloader07gen2.dfs.core.windows.net/fn-source-data"

schema_path = "abfss://autoloader-sink@autoloader07gen2.dfs.core.windows.net/dl-sink/schema"
checkpointlocation = "abfss://autoloader-sink@autoloader07gen2.dfs.core.windows.net/dl-sink/checkp"
tablelocation = "abfss://autoloader-sink@autoloader07gen2.dfs.core.windows.net/dl-sink/dltable"
deltaTable = "adb_learning.default.dltable"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trigger Modes

# COMMAND ----------

#Read and Write continuously (without trigger) 
(spark.readStream
.format("cloudFiles")
.option("cloudFiles.format", "csv")
.option("cloudFiles.schemaLocation", f"{schema_path}")
.option("cloudFiles.inferColumnTypes", "true")
.option("mergeSchema", "true")
.load(input_file)
.writeStream
.option("checkpointLocation", f"{checkpointlocation}")
.option("path", f"{tablelocation}")
.table(deltaTable)
)
    

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from   adb_learning.default.dltable

# COMMAND ----------

# Continous but Specifying time-based trigger intervals: will check every 30 seconds any new files arrived 
(spark.readStream
.format("cloudFiles")
.option("cloudFiles.format", "csv")
.option("cloudFiles.schemaLocation", f"{schema_path}")
.option("cloudFiles.inferColumnTypes", "true")
.option("mergeSchema", "true")
.load(input_file)
.writeStream
.option("checkpointLocation", f"{checkpointlocation}")
.option("path", f"{tablelocation}")
.trigger(processingTime = '60 seconds')
.table(deltaTable)
)

# COMMAND ----------

# trigger when file available
#In Databricks Runtime 11.3 LTS and above, the Trigger.Once setting is deprecated. Databricks recommends you use Trigger.AvailableNow for all incremental batch processing workloads
(spark.readStream
.format("cloudFiles")
.option("cloudFiles.format", "csv")
.option("cloudFiles.schemaLocation", f"{schema_path}")
.option("cloudFiles.inferColumnTypes", "true")
#.option("mergeSchema", "true")
.load(input_file)
.writeStream
.option("checkpointLocation", f"{checkpointlocation}")
.option("path", f"{tablelocation}")
.trigger(availableNow= True)
.table(deltaTable)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Options

# COMMAND ----------

bronzeDF = (spark.readStream
.format("cloudFiles")
.option("cloudFiles.format", "csv")
.option("cloudFiles.schemaLocation", f"{schema_path}") #The location to store the inferred schema and subsequent changes.
.option("cloudFiles.inferColumnTypes", "true") # By default, columns are inferred as strings when inferring JSON and CSV datasets
.option("cloudFiles.backfillInterval", "1 day") #backfills at a given interval, e.g. 1 day to backfill once a day, or 1 week to backfill once a week.
.option("cloudFiles.includeExistingFiles", "true") #include existing files in the stream processing input path or to only process new files arriving after initial setup. This option is evaluated only when you start a stream for the first time.
.option("cloudFiles.maxBytesPerTrigger", "3 GB") #When used together with cloudFiles.maxFilesPerTrigger, Databricks consumes up to the lower limit of cloudFiles.maxFilesPerTrigger or cloudFiles.maxBytesPerTrigger, whichever is reached first. 
.option ("cloudFiles.maxFilesPerTrigger", 1)
.option("cloudFiles.partitionColumns", "Yearcolumn") #Default value: None
.option ("cloudFiles.schemaEvolutionMode" ," ") # addNewColumns , rescue , failOnNewColumns , none ,Default value: "addNewColumns" when a schema is not provided. "none" otherwise.
.optionn ("cloudFiles.schemaHints", " ") # use schema hints to enforce the schema information that you know and expect on an inferred schema.
.option ("cloudFiles.validateOptions", "true") # Whether to validate Auto Loader options and return an error for unknown or inconsistent options.
.option("pathGlobfilter", "*.png") # to explicitly providing suffix patterns. The path only provides a prefix filter.
.option("mergeSchema", "true")
.load(input_file)
)

# COMMAND ----------

#pathGlobfilter 
bronzeDF = (spark.readStream
.format("cloudFiles")
.option("cloudFiles.format", "csv")
.option("cloudFiles.schemaLocation", f"{schema_path}")
.option("cloudFiles.inferColumnTypes", "true")
.option ("cloudFiles.schemaEvolutionMode" ,"none")
.option("mergeSchema", "true")
.option("pathGlobfilter", "*.csv") 
.load(input_file)
)

# COMMAND ----------

bronzeDF.writeStream\
.option("checkpointLocation", f"{checkpointlocation}")\
.option("path", f"{tablelocation}")\
.trigger(availableNow= True)\
.table(deltaTable)

# COMMAND ----------

partitioncolumn = "Month"

# COMMAND ----------

#partitioncolumn
(spark.readStream
.format("cloudFiles")
.option("cloudFiles.format", "csv")
.option("cloudFiles.schemaLocation", f"{schema_path}/schemalocation/")
.option("cloudFiles.inferColumnTypes", "true")
.option("partitionColumns", partitioncolumn ) 
#.option("mergeSchema", "true")
.load(input_file)
.writeStream
.option("checkpointLocation", f"{checkpointlocation}")
.option("path", f"{tablelocation}")
.option("partitionColumns", partitioncolumn ) 
.trigger(availableNow= True)
.table(deltaTable)
)

# COMMAND ----------

# .option("cloudFiles.schemaHints", "tags map<string,string>, version int")

# COMMAND ----------

# bronzeDF.writeStream\
# .option("checkpointLocation", f"{checkpointlocation}")\
# .option("path", f"{tablelocation}")\
# .trigger(availableNow= True)\
# .table(deltaTable)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pass Schema
# MAGIC ######spark.databricks.cloudFiles.schemaInference.sampleSize.numBytes (default 50 GB)
# MAGIC ######spark.databricks.cloudFiles.schemaInference.sampleSize.numFiles (default 1000 files)

# COMMAND ----------

# Readingfile for Schema
df = spark.read.format("csv").option("header" , "true").load("abfss://autoloader-source@autoloader07gen2.dfs.core.windows.net/fn-source-data/Organization_v2.csv")
# Schema into a Variable 
varschema = df.schema
print(varschema)

# COMMAND ----------

#Passing Schema variable

bronzeDF = (spark.readStream
.format("cloudFiles")
.option("cloudFiles.format", "csv")
.option("cloudFiles.schemaLocation", f"{schema_path}")
#.option("cloudFiles.inferColumnTypes", "true")
.option ("cloudFiles.schemaEvolutionMode" ,"none")
.option("mergeSchema", "true")
.option("pathGlobfilter", "*.csv")
.schema (varschema)
.load(input_file)
)

# COMMAND ----------

display(bronzeDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Evalution(Drift)

# COMMAND ----------

# addNewColumns --- (default) Stream fails. New columns are added to the schema. Existing columns do not evolve data types.
# rescue --- Schema is never evolved and stream does not fail due to schema changes. All new columns are recorded in the rescued data column.
# failOnNewColumns --- Stream fails. Stream does not restart unless the provided schema is updated, or the offending data file is removed.
# none  --- Does not evolve the schema, new columns are ignored, and data is not rescued unless the rescuedDataColumn option is set. Stream does not fail due to schema changes.

# COMMAND ----------

# When Auto Loader infers the schema, a rescued data column is automatically added to your schema as _rescued_data. You can rename the column or    include it in cases where you provide a schema by setting the option rescuedDataColumn

# COMMAND ----------

#2. rescue addNewColumns(default)
bronzeDF = (spark.readStream
.format("cloudFiles")
.option("cloudFiles.format", "csv")
.option("cloudFiles.schemaLocation", f"{schema_path}")
.option("cloudFiles.inferColumnTypes", "true")
.option ("cloudFiles.schemaEvolutionMode" ,"rescue")
.load(input_file)
)

# COMMAND ----------

display(bronzeDF)

# COMMAND ----------

#3. failOnNewColumns
bronzeDF1 = (spark.readStream
.format("cloudFiles")
.option("cloudFiles.format", "csv")
.option("cloudFiles.schemaLocation", f"{schema_path}")
.option("cloudFiles.inferColumnTypes", "true")
.option ("cloudFiles.schemaEvolutionMode" ,"failOnNewColumns")
.option("mergeSchema", "true")
.option("pathGlobfilter", "*.csv") 
.load(input_file)
)

# COMMAND ----------

display(bronzeDF1 , truncate = False)

# COMMAND ----------

#4. none 
bronzeDF2 = (spark.readStream
.format("cloudFiles")
.option("cloudFiles.format", "csv")
.option("cloudFiles.schemaLocation", f"{schema_path}")
.option("cloudFiles.inferColumnTypes", "true")
.option ("cloudFiles.schemaEvolutionMode" ,"none")
.load(input_file)
)

# COMMAND ----------

display(bronzeDF2, turncate= False)

# COMMAND ----------

#1. addNewColumns. Stream fails. New columns are added to the schema. Existing columns do not evolve data types.
bronzeDF3 = (spark.readStream
.format("cloudFiles")
.option("cloudFiles.format", "csv")
.option("cloudFiles.schemaLocation", f"{schema_path}")
.option("cloudFiles.inferColumnTypes", "true")
.option ("cloudFiles.schemaEvolutionMode" ,"addNewColumns")
#.option("mergeSchema", "true")
#.option("pathGlobfilter", "*.csv") 
.load(input_file)
)

# COMMAND ----------

display(bronzeDF3 ,truncate = False)

# COMMAND ----------

for s in spark.streams.active:
    print(f"Stopping StreamID : {s}")
    s.stop()
    s.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Common data loading patterns

# COMMAND ----------

# ?              Matches any single character
# *              Matches zero or more characters
# [abc]          Matches a single character from character set {a,b,c}
# [a-z]          Matches a single character from the character range {a…z}
# [^a]           Matches a single character that is not from character set or range {a}. Note that the ^ character must occur immediately to the right of the opening bracket.
# {ab,cd}        Matches a string from the string set {ab, cd}.
# {ab,c{de, fh}} Matches a string from the string set {ab, cde, cfh}.

# COMMAND ----------

# MAGIC %md
# MAGIC ### End

# COMMAND ----------

#Use the path for providing prefix patterns
bronzeDF = (spark.readStream
.format("cloudFiles")
.option("cloudFiles.format", "csv")
.option("cloudFiles.schemaLocation", f"{output_path}/schemalocation/")
.option("cloudFiles.inferColumnTypes", "true")
.option ("cloudFiles.schemaEvolutionMode" ,"none")
.option("mergeSchema", "true")
.load(f"{input_file}/*/")
)

# COMMAND ----------

# MAGIC %fs ls "/databricks-datasets"

# COMMAND ----------

#if need to apply any transformation on streaming data
bronzeDF = (spark.readStream
.format("cloudFiles")
.option("cloudFiles.format", "csv")
.option("cloudFiles.schemaLocation", f"{schema_path}")
.option("cloudFiles.inferColumnTypes", "true")
#.option("mergeSchema", "true")
.load(input_file)
)
#Apply transformation before persisting data to delta table
bronzeDF.writeStream\
.option("checkpointLocation", f"{checkpointlocation}")\
.option("path", f"{tablelocation}")\
.trigger(availableNow= True)\
.table(deltaTable)

# COMMAND ----------

(spark.readStream
.format("cloudFiles")
.option("cloudFiles.format", "csv")
.option("cloudFiles.schemaLocation", f"{schema_path}")
.option("cloudFiles.inferColumnTypes", "true")
#.option("mergeSchema", "true")
.load(input_file)
.writeStream
.option("checkpointLocation", f"{checkpointlocation}")
.option("path", f"{tablelocation}")
.trigger(availableNow= True)
.table(deltaTable)
)
