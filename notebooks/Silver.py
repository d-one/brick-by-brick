# Databricks notebook source
# MAGIC %md
# MAGIC # Read a table from Unity Catalog
# MAGIC Namespace
# MAGIC * Catalog = sds_catalog
# MAGIC * Schema = default
# MAGIC * Table = laptop_data

# COMMAND ----------

df_bronze = spark.table("robert_yousif.bronze.laptop_prices")
display(df_bronze)

# COMMAND ----------

df_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # Clean & Augment the data

# COMMAND ----------

from pyspark.sql.functions import expr
from pyspark.sql.types import StringType, DateType, FloatType

df_laptop_bronze = spark.table("robert_yousif.bronze.laptop_prices")

df_laptop_silver = (
    df_bronze.withColumn("Inches", df_bronze["Inches"].cast(FloatType()))
    .withColumn("Price", df_bronze["Price"].cast(FloatType()))
    .withColumn("_c0", df_bronze["_c0"].cast(IntegerType()))
    .withColumnRenamed("_c0", "Id")
    .withColumn("Weight", expr("substring(Weight, 0, length(Weight)-2)").cast(FloatType()))
    .withColumnRenamed("Weight", "Weight_Kg")
    .withColumnRenamed("Price", "Price_INR")

)


display(df_laptop_silver)

# COMMAND ----------

# MAGIC %md 
# MAGIC # Writing the dataframe to Unity Catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Your Silver Schema
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS robert_yousif.silver

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Save the dataframe as a delta table inside the unity catalog

# COMMAND ----------

catalog_name = "robert_yousif"
schema_name = "silver"
table_name = "laptop_prices"

df_laptop_silver.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog_name}.{schema_name}.{table_name}")


# COMMAND ----------

display(spark.table(f"{catalog_name}.{schema_name}.{table_name}"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Exercise
# MAGIC Go check out the lineage of the table produced by Unity Catalog inside the Data Explorer
# MAGIC 1. Click on the `Data` tab in the left panel
# MAGIC 2. Find your table
# MAGIC 3. Check out the lineage table and graph for both `Tables` and `Notebooks`

# COMMAND ----------

# MAGIC %md
# MAGIC # Excercise
# MAGIC What happens if you need to change the schema of a silver dataframe and want to append or overwrite it to the target silver table? 
# MAGIC * Check out the option [`mergeSchema`](https://www.databricks.com/blog/2019/09/24/diving-into-delta-lake-schema-enforcement-evolution.html)
# MAGIC
# MAGIC Lets change one column name and see how the write behaves

# COMMAND ----------

df_laptop_silver_mergeSchema = (
    df_laptop_silver.withColumnRenamed("Weight", "Weight_kilogram")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Without mergeSchema, you have to run this before the next command to get the error, why is that?

# COMMAND ----------

(df_laptop_silver_mergeSchema
    .write.format("delta")
    .mode("overwrite")
    .saveAsTable(f"{catalog_name}.{schema_name}.{table_name}"))

# COMMAND ----------

# MAGIC %md
# MAGIC With mergeSchema

# COMMAND ----------

df_laptop_silver_mergeSchema.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog_name}.{schema_name}.{table_name}")
