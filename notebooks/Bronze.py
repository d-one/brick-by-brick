# Databricks notebook source
# MAGIC %md 
# MAGIC # Reading the data 
# MAGIC In this tutorial you can use the Repos to read the data, as the data is part of the repository [sds-brick-by-brick](https://github.com/d-one/sds-brick-by-brick).
# MAGIC
# MAGIC Please change the name inside the `path` to your own.
# MAGIC The dataset is called `laptop_data.csv`.

# COMMAND ----------

# MAGIC %md
# MAGIC Let's check if you can source the file using [dbutils](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/databricks-utils)

# COMMAND ----------

path = "file:/Workspace/Repos/robert.yousif@ms.d-one.ai/sds-brick-by-brick/data/laptop_data.csv"

dbutils.fs.ls(path)

# COMMAND ----------

# MAGIC %md 
# MAGIC Now lets load the data into a spark dataframe & display it.

# COMMAND ----------

try:
    df_laptop_raw = spark.read.format("csv").option("header", "true").load(path)
except:
    print("File does not exist, please make sure that your path is correct and that you have pulled the repository to databricks repos")

# COMMAND ----------

display(df_laptop_raw)

# COMMAND ----------

# MAGIC %md 
# MAGIC # Writing a delta table to Unity Catalog

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Save the dataframe as a delta table inside the unity catalog

# COMMAND ----------

catalog_name = "sds_catalog"
schema_name = "robert_yousif"
table_name = "bronze"

df_laptop_raw.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.{table_name}")


# COMMAND ----------

# MAGIC %md
# MAGIC With the command above we are:
# MAGIC * Specifying the format to be `delta`
# MAGIC * Specifying `mode` to `overwrite` which will write over any existing data on the table (if any, otherwise it will get created)

# COMMAND ----------

# MAGIC %md 
# MAGIC Load the data from the Unity Catalog to see that it exists.

# COMMAND ----------

df_laptop_bronze = spark.table(f"{catalog_name}.{schema_name}.{table_name}")


# COMMAND ----------

# MAGIC %md
# MAGIC Load the data using SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sds_catalog.robert_yousif.bronze

# COMMAND ----------

# MAGIC %md
# MAGIC Read the data using embedded SQL

# COMMAND ----------

df_embedded = spark.sql(f"SELECT * FROM {catalog_name}.{schema_name}.{table_name}")
display(df_embedded)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Exercise
# MAGIC * Create a new table called `bronze_x2`
# MAGIC * Write the `df_laptop` twice to the table `bronze_x2`
# MAGIC * Run the assertion below to make sure you twice the amount of data as in table `bronze`

# COMMAND ----------

# Do exercise here





# COMMAND ----------

df_laptop_bronze_x2 = spark.table("bronze_x2")
assert df_laptop_bronze.count() == df_laptop_bronze_x2.count()*2, "Your bronze_x2 table is not twice the size"
# If there is no error message, your assertion is true.

# COMMAND ----------

# MAGIC %md 
# MAGIC Check the delta table history of your bronze_x2 table

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY sds_catalog.robert_yousif.bronze_x2

# COMMAND ----------

# MAGIC %md
# MAGIC # Conclusion
# MAGIC * In this notebook you learned how to read a csv file from the Repos
# MAGIC * Read a csv file and store it in a spark dataframe
# MAGIC * Write the dataframe as a UC table inside your own schema
# MAGIC * How to use SQL and embedded SQL in a python notebook
# MAGIC
# MAGIC **Next:** Go to the Silver Notebook and continue from there
