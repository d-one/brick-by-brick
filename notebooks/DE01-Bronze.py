# Databricks notebook source
# MAGIC %md 
# MAGIC # Reading the data 
# MAGIC In this tutorial you can use the Repos to read the data, as the data is part of the repository [brick-by-brick](https://github.com/d-one/brick-by-brick).
# MAGIC
# MAGIC Please change the name inside the `path` to your own.
# MAGIC The dataset is called `laptop_data.csv`.

# COMMAND ----------

# MAGIC %md
# MAGIC Let's check if you can source the file using [dbutils](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/databricks-utils)
# MAGIC
# MAGIC Make sure to change the `PATH` to your own directory.

# COMMAND ----------

# set up the below params
user_email = "spyros.cavadias@ms.d-one.ai"
catalog_name = "spyros_cavadias

# COMMAND ----------

path = f"file:/Workspace/Repos/{user_email}/brick-by-brick/data/laptop_price_euro.csv"

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

spark.sql(
    f"""
    CREATE CATALOG IF NOT EXISTS {catalog_name}
    """
)

# COMMAND ----------

spark.sql(
    f"""
    CREATE SCHEMA IF NOT EXISTS {catalog_name}.bronze
    """
)

# COMMAND ----------

# MAGIC %md 
# MAGIC # Writing a delta table to Unity Catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Your Bronze Schema
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Save the dataframe as a delta table inside the unity catalog

# COMMAND ----------

schema_name = "bronze"
table_name = "laptop_prices_euro"

df_laptop_raw.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.{table_name}")


# COMMAND ----------

# MAGIC %md
# MAGIC With the command above we are:
# MAGIC * Specifying the format to be `delta`
# MAGIC * Specifying `mode` to `overwrite` which will write over any existing data on the table (if any, otherwise it will get created)
# MAGIC
# MAGIC
# MAGIC What would be the difference between `append` and `overwrite` in terms of the:
# MAGIC   * How your table would look like?
# MAGIC   * The space used for the data? (Think about how historization and vacuum works)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Read the data
# MAGIC Load the data from the Unity Catalog to see that it exists.

# COMMAND ----------

df_laptop_bronze = spark.table(f"{catalog_name}.{schema_name}.{table_name}")
display(df_laptop_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC Read the data using embedded SQL

# COMMAND ----------

df_embedded = spark.sql(f"SELECT * FROM {catalog_name}.{schema_name}.{table_name}")
display(df_embedded)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exit notebook when running as a workflow task

# COMMAND ----------

dbutils.notebook.exit("End of notebook when running as a workflow task")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Exercise
# MAGIC * Create a new table called `laptop_prices_dev`
# MAGIC * Write the `df_laptop` to the table `laptop_prices_dev` using both `append` and `overwrite` to create some history.
# MAGIC * Check the table history columns `operation`& `operationMetrics`

# COMMAND ----------

# Do exercise here





# COMMAND ----------

# MAGIC %md 
# MAGIC Check the delta table history of your `laptop_prices_dev`

# COMMAND ----------

df_history = spark.sql(
    f"""
    DESCRIBE HISTORY {catalog_name}.{schema_name}.{table_name}
    """
)

display(df_history)

# COMMAND ----------

# MAGIC %md
# MAGIC # Conclusion
# MAGIC * In this notebook you learned how to read a csv file from the Repos
# MAGIC * Read a csv file and store it in a spark dataframe
# MAGIC * Write the dataframe as a UC table inside your own schema
# MAGIC * How to use SQL and embedded SQL in a python notebook
# MAGIC
# MAGIC **Next:** Go to the Silver Notebook and continue from there
