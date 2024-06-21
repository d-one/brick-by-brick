# Databricks notebook source
# MAGIC %md
# MAGIC # **Ingesting Kafka Streaming Data**

# COMMAND ----------

# MAGIC %md
# MAGIC Importing the necessary libraries

# COMMAND ----------

# import libraries
import pyspark.sql.types as t
import pyspark.sql.functions as f

# COMMAND ----------

# ********* workflow parameters ********* #
# set parameters here only if running notebook, for example:
# dbutils.widgets.text("CATALOG_NAME", "konstantinos_ninas")

# COMMAND ----------

# set up catalog name either by workflow parameters or by using current user's id
user_email = spark.sql('select current_user() as user').collect()[0]['user']
try:
    catalog_name = dbutils.widgets.get("CATALOG_NAME")
except:
    catalog_name = user_email.split('@')[0].replace(".", "_").replace("-", "_")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option 1 - Ingesting from Azure Container as batch
# MAGIC
# MAGIC We ingest Kafka generated data that land on an Azure Container, as batch. This can be combined in a workflow with schedulers to streamline the process

# COMMAND ----------

# MAGIC %md
# MAGIC Setting up the connection to the Azure Container

# COMMAND ----------

# Configuration details
storage_account_name = ""
container_name = ""
storage_account_access_key = ""

# Mounting the blob storage
dbutils.fs.mount(
source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/",
mount_point = f"/mnt/{container_name}",
extra_configs = {f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_account_access_key}
)

# COMMAND ----------

# Verify the mount
display(dbutils.fs.ls(f"/mnt/{container_name}"))

# COMMAND ----------

# specify the expected schema of the csv files in the Blob Storage
schema = (t.StructType()
      .add("RecommendationTypeId",t.IntegerType(),True)
      .add("RecommendationGameId",t.IntegerType(),True)
      .add("ActionId",t.IntegerType(),True)
      .add("UserId",t.IntegerType(),True)
      .add("Date",t.DateType(),True)
)

# COMMAND ----------

# read all the tables needed
raw_feedback_sdf = (spark
                    .read
                    .format("csv")
                    .option("header", "true")
                    .option("delimiter",";")
                    .schema(schema)
                    .option("recursiveFileLookup", "true")
                    .option("pathGlobFilter","*.csv")
                    .load("dbfs:/mnt/streamingcontainer/")
)

existing_feedback_sdf = spark.read.table(f"{catalog_name}.bronze.liveactions")

# COMMAND ----------

# MAGIC %md
# MAGIC Display the ingested data

# COMMAND ----------

raw_feedback_sdf.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option 2 - Ingesting from Azure Container real-time
# MAGIC
# MAGIC We ingest Kafka generated data that land on an Azure Container, real-time, as they come

# COMMAND ----------

# for streaming table solution
raw_feedback_live_sdf = (spark
                    .readStream
                    .option("header", "true")
                    .option("delimiter",";")
                    .schema(schema)
                    .csv("dbfs:/mnt/streamingcontainer/")
)

# COMMAND ----------

display(raw_feedback_live_sdf)

# COMMAND ----------

# append new rows - finding the delta 
query = (
    raw_feedback_sdf
    .join(
        raw_feedback_sdf
        , ["RecommendationGameId", "UserId", "Date"]
        , "left_anti"
    )
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(f"{catalog_name}.bronze.liveactions")
)

# COMMAND ----------

# # writing the table as Streaming
# query = (
#   df.writeStream
#     .format("delta")
#     .outputMode("append")
#     .trigger(processingTime='2 seconds')
#     .option("checkpointLocation", f"/tmp/delta/events/_checkpoints/feedback_collection_{_ENVIRONMENT}")
#     .toTable(f"{catalog_name}.bronze.liveactions")
# )

# query.awaitTermination()

