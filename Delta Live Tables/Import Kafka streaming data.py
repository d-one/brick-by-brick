# Databricks notebook source
# MAGIC %md
# MAGIC # **Ingesting Kafka Streaming Data using Delta Live Tables**

# COMMAND ----------

# MAGIC %md
# MAGIC Import necessary libraries

# COMMAND ----------

pip install dlt

# COMMAND ----------

# import libraries
import pyspark.sql.types as t
import pyspark.sql.functions as f
import dlt

# COMMAND ----------

# MAGIC
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

# MAGIC %md
# MAGIC ## Option 1 - read the kafka bus from an Azure container

# COMMAND ----------

# MAGIC %md
# MAGIC Create DLT Tables using python

# COMMAND ----------

# setting up the dlt loader
@dlt.table
def live_feedback():
  return (
      spark
      .readStream
      .option("header", "true")
      .option("delimiter",";")
      .csv("dbfs:/mnt/streamingcontainer/")
  )

# COMMAND ----------

# MAGIC %md
# MAGIC We do the same using SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING TABLE live_feedback
# MAGIC AS SELECT * FROM cloud_files("dbfs:/mnt/streamingcontainer/", "csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option 2 - Read the kafka bus directly

# COMMAND ----------

# setting up the dlt table using a kafka connector
@dlt.table
def kafka_raw():
  return (
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "<server:ip>")
      .option("subscribe", "topic1")
      .option("startingOffsets", "latest")
      .load()
  )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH LIVE TABLE streaming_silver_table(
# MAGIC   CONSTRAINT valid_response EXPECT (Feedback_Id IN (0, 1)),
# MAGIC   CONSTRAINT not_null_user_name EXPECT (UserId > 0) ON VIOLATION FAIL UPDATE
# MAGIC )
# MAGIC COMMENT "CLEAN THE RAW DATA BY REMOVING INVALID RESPONSES & FEEDBACK WITH NULL USERIDs"
# MAGIC AS SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   LIVE.kafka_raw
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH LIVE TABLE streaming_silver_table
# MAGIC COMMENT "BUSINESS - SERVING LEVEL AGGREGATIONS"
# MAGIC AS SELECT
# MAGIC   UserId, count(*) as FeedbackResponses
# MAGIC FROM
# MAGIC   LIVE.streaming_silver_table
# MAGIC GROUP BY UserId
# MAGIC ;
