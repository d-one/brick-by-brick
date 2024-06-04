# Databricks notebook source
# MAGIC %md
# MAGIC # Ingesting Data from Azure
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading a Data warehouse table

# COMMAND ----------

# import libraries
import pyspark.sql.types as t
import pyspark.sql.functions as f

# COMMAND ----------

# set up the variables needed to access Azure resources using secrets
spark=spark
jdbc_username=dbutils.secrets.get(scope="<SCOPE>", key="<SCOPE-USERNAME-KEY>")
jdbc_password=dbutils.secrets.get(scope="<SCOPE>", key="<SCOPE-PASSWORD-KEY>")
jdbc_conn=dbutils.secrets.get(scope="<SCOPE>", key="<SCOPE-CONNECTION-KEY>")
jdbc_schema= "staging"
target_schema = "bronze"
tablename = "active_users"

# COMMAND ----------

# query a dwh table using secrets extracted above
ingested_users_sdf = (spark.read.format("jdbc")
            .option("url", jdbc_conn)
            .option(
                "query",
                f"""
                SELECT 
                    UserId,
                    Username,
                    FirstName,
                    LastName,
                    BirthDate
                FROM {jdbc_schema}.{tablename}
                """,
            )
            .option("user", jdbc_username)
            .option("password", jdbc_password)
            .load()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading files from Azure Blob Storage using SAS Token

# COMMAND ----------

# receive azure account and sas token from secrets
azure_account = dbutils.secrets.get(scope="<SCOPE>", key="SCOPE-ACCOUNT-KEY")
azure_sas_token = dbutils.secrets.get(scope="<SCOPE>", key="SCOPE-SAS-TOKEN-KEY")
path = "directory/users_folder"
ingestion_layer="staging"

# COMMAND ----------

# azure connection setup
spark.conf.set(
    f"fs.azure.account.auth.type.{azure_account}.dfs.core.windows.net", "SAS"
)
spark.conf.set(
    f"fs.azure.sas.token.provider.type.{azure_account}.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider",
)
spark.conf.set(
    f"fs.azure.sas.fixed.token.{azure_account}.dfs.core.windows.net", azure_sas_token
)

# COMMAND ----------

# specify the expected schema of the csv files in the Blob Storage
schema = (t.StructType()
      .add("UserId",t.IntegerType(),True)
      .add("Username",t.StringType(),True)
      .add("FirstName",t.StringType(),True)
      .add("LastName",t.StringType(),True)
      .add("BirthDate",t.DateType(),True)
)

# COMMAND ----------

# ingest raw data and transform them
ingested_users_sdf = (
  spark.read
  .format("csv")
  .option("header", True)
  .schema(schema)
  .load(f"abfss://{ingestion_layer}@{azure_account}.dfs.core.windows.net/{path}")
  .withColumn("FullName", f.concat_ws(" ", f.col("FirstName"), f.col("LastName")))
)

# COMMAND ----------

# write ingested files
(
    ingested_users_sdf
    .saveAsTable(f"{target_schema}.{tablename}", mode="overwrite")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mounting blob storage to databricks catalog

# COMMAND ----------

# Configuration details
storage_account_name = "your_storage_account_name"
container_name = "your_container_name"
storage_account_access_key = "your_storage_account_access_key"

# Mounting the blob storage
dbutils.fs.mount(
source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/",
mount_point = f"/mnt/{container_name}",
extra_configs = {f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_account_access_key}
)

# Verify the mount
display(dbutils.fs.ls(f"/mnt/{container_name}"))
