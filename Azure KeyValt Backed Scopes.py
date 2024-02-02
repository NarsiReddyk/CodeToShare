# Databricks notebook source
# DBTITLE 1,Create an Azure Key Vault-backed secret scope
# MAGIC %md 
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes#create-an-azure-key-vault-backed-secret-scope-using-the-ui
# MAGIC
# MAGIC Go to 'https://{databricks-instance}#secrets/createScope'. This URL is case sensitive; scope in createScope must be uppercase.

# COMMAND ----------

#list Scopes
dbutils.secrets.listScopes()

# COMMAND ----------

#List secrets in a scope
dbutils.secrets.list("demoscope7")

# COMMAND ----------

#To get a single secret
dbutils.secrets.get(scope = "demoscope7" , key = "sqldbpw")

# COMMAND ----------

#To use in Notebook Code or workflow
password  = dbutils.secrets.get(scope = "demoscope7" , key = "sqldbpw")

# COMMAND ----------

# DBTITLE 1,Entering Credentials Directly in Code
storageAccountName = "datastorage17"
storageAccountAccessKey = 'hjh9W8oDGLc5g+GBxNO5aPfLdODS5cW15CiBs5tEV6+v8Ebu0H2K/VbVHWK7vWRZFpuV0MiDGC/C+AStwCeE6A=='
blobContainerName = "datacontainer"
mountPoint = "/mnt/datacontainer/"
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
  try:
    dbutils.fs.mount(
      source = "wasbs://datacontainer@datastorage17.blob.core.windows.net".format(blobContainerName, storageAccountName),
      mount_point = mountPoint,
      extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey}
    )
    print("mount succeeded!")
  except Exception as e:
    print("mount exception", e)

# COMMAND ----------

#Getting Storage account access key from Keyvault
demostoragekey = dbutils.secrets.get(scope = "test01" , key = "demostoragekey")

# COMMAND ----------

# DBTITLE 1,Using Secrets Scope for credential 
storageAccountName = "datastorage17"
storageAccountAccessKey = demostoragekey
blobContainerName = "datacontainer"
mountPoint = "/mnt/datacontainerwithscope/"
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
  try:
    dbutils.fs.mount(
      source = "wasbs://datacontainer@datastorage17.blob.core.windows.net".format(blobContainerName, storageAccountName),
      mount_point = mountPoint,
      extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey}
    )
    print("mount succeeded!")
  except Exception as e:
    print("mount exception", e)

# COMMAND ----------

#if you have sufficient permissions
password  = dbutils.secrets.get(scope = "test01" , key = "demostoragekey")

# COMMAND ----------

for passwordValue in password:
    print(passwordValue, end=",")
