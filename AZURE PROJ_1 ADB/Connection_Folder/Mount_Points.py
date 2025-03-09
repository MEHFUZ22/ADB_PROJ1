# Databricks notebook source
# Fetch secrets from Databricks secrets
container_name = dbutils.secrets.get(scope="azure-proj", key="containername").strip()
storage_account_name = dbutils.secrets.get(scope="azure-proj", key="storageaccountname").strip()
sas_token = dbutils.secrets.get(scope="azure-proj", key="sas-source").strip()

# Define the SAS token configuration
config_key = f"fs.azure.sas.{container_name}.{storage_account_name}.blob.core.windows.net"
configs = {config_key: sas_token}

# Define the source URL for Azure Blob Storage
source_url = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net"

# Define the mount point
mount_point = "/mnt/source_blob/"

# Check if the mount point already exists
if not any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
    try:
        # Mount Azure Blob Storage using the SAS token
        dbutils.fs.mount(
            source=source_url,
            mount_point=mount_point,
            extra_configs=configs
        )
        print(f"Azure Blob Storage mounted at {mount_point}")
    except Exception as e:
        print(f"Failed to mount Azure Blob Storage: {str(e)}")
else:
    print(f"Mount point {mount_point} already exists.")

# COMMAND ----------

# MAGIC %scala
# MAGIC val containerName = dbutils.secrets.get(scope="azure-proj",key="containername-manual")
# MAGIC val storageAccountName = dbutils.secrets.get(scope="azure-proj",key="storageaccountname")
# MAGIC val sas = dbutils.secrets.get(scope="azure-proj",key="sas-manualfiles")
# MAGIC val config = "fs.azure.sas." + containerName+ "." + storageAccountName + ".blob.core.windows.net"
# MAGIC
# MAGIC dbutils.fs.mount(
# MAGIC   source = dbutils.secrets.get(scope="azure-proj",key="blob-mnt-path-manualfiles"),
# MAGIC   mountPoint = "/mnt/manualfiles_blob/",
# MAGIC   extraConfigs = Map(config -> sas))

# COMMAND ----------

# MAGIC %py
# MAGIC configs = {"fs.azure.account.auth.type": "OAuth",
# MAGIC           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
# MAGIC           "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope = "azure-proj", key = "spn-app-id"),
# MAGIC           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="azure-proj",key="spn-client-secret"),
# MAGIC           "fs.azure.account.oauth2.client.endpoint": dbutils.secrets.get(scope = "azure-proj", key = "data-client-refresh-url")}
# MAGIC
# MAGIC #Optionally, you can add <directory-name> to the source URI of your mount point.
# MAGIC mountPoint="/mnt/raw_datalake/"
# MAGIC if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
# MAGIC   dbutils.fs.mount(
# MAGIC     source ="abfss://raw@{}.dfs.core.windows.net/".format(dbutils.secrets.get(scope="azure-proj", key="datalake-raw")),
# MAGIC     mount_point = mountPoint,
# MAGIC     extra_configs = configs)

# COMMAND ----------

# MAGIC %py
# MAGIC configs = {"fs.azure.account.auth.type": "OAuth",
# MAGIC           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
# MAGIC           "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope = "azure-proj", key =  "spn-app-id"),
# MAGIC           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="azure-proj",key="spn-client-secret"),
# MAGIC           "fs.azure.account.oauth2.client.endpoint": dbutils.secrets.get(scope = "azure-proj", key = "data-client-refresh-url")}
# MAGIC
# MAGIC #Optionally, you can add <directory-name> to the source URI of your mount point.
# MAGIC mountPoint="/mnt/cleansed_datalake/"
# MAGIC if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
# MAGIC   dbutils.fs.mount(
# MAGIC     source ="abfss://cleansed@{}.dfs.core.windows.net/".format(dbutils.secrets.get(scope="azure-proj", key="datalake-cleansed")),
# MAGIC     mount_point = mountPoint,
# MAGIC     extra_configs = configs)

# COMMAND ----------

# MAGIC %py
# MAGIC configs = {"fs.azure.account.auth.type": "OAuth",
# MAGIC           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
# MAGIC           "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope = "azure-proj", key = "data-app-id"),
# MAGIC           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="azure-proj",key="data-app-secrect"),
# MAGIC           "fs.azure.account.oauth2.client.endpoint": dbutils.secrets.get(scope = "azure-proj", key = "data-client-refresh-url")}
# MAGIC
# MAGIC #Optionally, you can add <directory-name> to the source URI of your mount point.
# MAGIC mountPoint="/mnt/mart_datalake/"
# MAGIC if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
# MAGIC   dbutils.fs.mount(
# MAGIC     source = dbutils.secrets.get(scope = "azure-proj", key = "datalake-mart"),
# MAGIC     mount_point = mountPoint,
# MAGIC     extra_configs = configs)
