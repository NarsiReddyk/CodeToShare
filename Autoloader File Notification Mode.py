# Databricks notebook source
# MAGIC %md
# MAGIC ####Variables

# COMMAND ----------


#https://medium.com/@shubhodaya.hampiholi/streaming-data-ingestion-with-databricks-auto-loader-8216d4ad3b67
#https://stephendavidwilliams.com/incremental-ingestion-with-databricks-autoloader-via-file-notifications

input_file = "abfss://autoloader-source@autoloader07gen2.dfs.core.windows.net/fn-source-data"

schema_path = "abfss://autoloader-sink@autoloader07gen2.dfs.core.windows.net/fn-sink/schema"
checkpointlocation = "abfss://autoloader-sink@autoloader07gen2.dfs.core.windows.net/fn-sink/checkp"
tablelocation = "abfss://autoloader-sink@autoloader07gen2.dfs.core.windows.net/fn-sink/fntable"
deltaTable = "adb_learning.default.fntable"


# COMMAND ----------

streaming_df = (spark.readStream
             .format("cloudFiles")  # Tells Spark to use Autoloader
             .option("cloudFiles.format", "csv") # The actual format of raw data file
             .option("cloudFiles.schemaLocation", schema_path) # The storage path were schema files will be saved
             .option("cloudFiles.schemaEvolutionMode" , "none")
             .option("cloudFiles.clientId", "3948945f-228e-474f-a16a-1bf9be7cb80d") # The application/client ID of the Service Principal(adb-autoloader)
             .option("cloudFiles.clientSecret", "1Ta8Q~il1-5IH6AHBrc4wk-kr_NpV.6I-QX82dvd") # The client secret value
             .option("cloudFiles.tenantId", "06be16f7-967d-41df-a35b-1af0a221ef2d") # The tenant ID
             .option("cloudFiles.subscriptionId", "1ddbae92-b2c1-43b3-92ca-49e8cff526c2") # The Azure resource subscription ID
             .option("cloudFiles.connectionString","DefaultEndpointsProtocol=https;AccountName=autoloader07gen2;AccountKey=ByyoG+ZwidjfxjPrn64k81PnIGy69ob8w7/3jvzpOwXyggDF+vT9G3Sy6KptIUnbUaAn/fyQeDg++ASt7hLdjg==")
             .option("cloudFiles.resourceGroup", "adb-learning-rg") # The Azure Resource Group Name
             .option("cloudFiles.useNotifications", True) # Has to be set to true to enable file notification mode
             .option("header", True)
             .load(input_file) # The actual source data file path
)

# COMMAND ----------

#Wrtie to Delta table
(streaming_df.writeStream
.option("checkpointLocation", f"{checkpointlocation}")
.option("path", f"{tablelocation}") #Sink location
#.trigger(processingTime = '30 seconds')
.table(deltaTable)) #sink table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from adb_learning.default.fntable

# COMMAND ----------

for s in spark.streams.active:
    print(f"Stopping StreamID : {s}")
    s.stop()
    s.awaitTermination()

# COMMAND ----------

display(streaming_df)

# COMMAND ----------

display(spark.read.json(checkpointlocation ))

# COMMAND ----------

display(spark.read.json(schema_path + "/checkp"))

# COMMAND ----------

# MAGIC %md
# MAGIC ####End

# COMMAND ----------

# MAGIC %sql
# MAGIC --Check data from delta table
# MAGIC SELECT count(*) FROM delta.`/mnt/sink/autoloader_demodb/filenotification`

# COMMAND ----------

#Variables to hold service principal and access keys Ideally should get them from Key-vault
client_id = "9c9d4e3a-5083-4b1d-929c-f5ec31af7791"
client_secret =  "iO-8Q~.AFZJcArEaMcftWPzOb.Xik_s9rmry3ba0"
tenant_id = "1fbd65bf-5def-4eea-a692-a089c255346b"
subscription_id = "f3242b6b-5baa-4404-a4c6-ba435848ddbe"
connection_string = "DefaultEndpointsProtocol=https;AccountName=datahubgen2;AccountKey=d9lv +mnncDfGX5HVQzA1uND9QksY5TQiT8q8uqFe46dFTyTOi0bpdYpo+SPRCFUk0b3iVF/8Fk2E+ASt9PRgXw==;EndpointSuffix=core.windows.net"
resource_group = "learning"
schema_path = "/mnt/sink/autoloader_demodb/filenotification/schema/"
source_directory = "dbfs:/mnt/source/csv_auto"


# COMMAND ----------

#Read data files
streaming_df = (spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "csv")
             .option("cloudFiles.schemaLocation", schema_path)
             .option("cloudFiles.clientId", client_id)
             .option("cloudFiles.clientSecret", client_secret)
             .option("cloudFiles.tenantId", tenant_id)
             .option("cloudFiles.subscriptionId", subscription_id)
             .option("cloudFiles.connectionString", connection_string)
             .option("cloudFiles.resourceGroup", resource_group)
             .option("cloudFiles.useNotifications", False)
             .option("cloudFiles.validateOptions", False)
             .option("cloudFiles.schemaEvolutionMode", "none")
             .option("header", True)
             .load(source_directory)
           )

