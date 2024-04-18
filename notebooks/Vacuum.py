# Databricks notebook source
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

# COMMAND ----------

spark.sql(
    "create or replace table panagiotis_goumenakis.default.nyc_trip_duration deep clone opap_catalog.default.nyc_trip_duration"
)

# COMMAND ----------

sdf = spark.read.table("panagiotis_goumenakis.default.nyc_trip_duration")
sdf2 = sdf.limit(2)

# COMMAND ----------

sdf2.write.mode("overwrite").saveAsTable("panagiotis_goumenakis.default.nyc_trip_duration")

# COMMAND ----------

spark.sql("VACUUM panagiotis_goumenakis.default.nyc_trip_duration RETAIN 0.000001 hours")

# COMMAND ----------

spark.sql("drop table panagiotis_goumenakis.default.nyc_trip_duration")
