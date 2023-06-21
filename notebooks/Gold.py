# Databricks notebook source
# MAGIC %md
# MAGIC # Read a table from Unity Catalog
# MAGIC In this notebook we will do some aggregations

# COMMAND ----------

# set up the below params
catalog_name = "robert_yousif"

# COMMAND ----------

df_silver = spark.table(f"{catalog_name}.silver.laptop_prices")
display(df_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Make some aggregations 

# COMMAND ----------

# MAGIC %md
# MAGIC You can write it as pure SQL or embedded SQL. You can also do the aggregations using pure pyspark.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   Company,
# MAGIC   count(Company) as num_of_laptops,
# MAGIC   AVG(Price_euros) as avg_price,
# MAGIC   AVG(Weight_kg/Inches) as weight_per_inch
# MAGIC FROM
# MAGIC   robert_yousif.silver.laptop_prices
# MAGIC GROUP BY
# MAGIC   Company
# MAGIC ORDER BY
# MAGIC   num_of_laptops DESC

# COMMAND ----------



# COMMAND ----------

df_laptop_gold_1 = spark.sql(
    f"""
    SELECT
        Company,
        count(Company) as num_of_laptops,
        AVG(Price_euros) as avg_price
    FROM
        {catalog_name}.silver.laptop_prices
    GROUP BY
        Company
    ORDER BY
        num_of_laptops DESC                   
    """
)
display(df_laptop_gold_1)

# COMMAND ----------

# MAGIC %md 
# MAGIC # Writing the dataframe to Unity Catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Your Gold Schema
# MAGIC

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.gold")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Save the dataframe as a delta table inside the unity catalog

# COMMAND ----------

schema_name = "gold"
table_name = "laptop_prices_1"

df_laptop_gold_1.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog_name}.{schema_name}.{table_name}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Exit notebook when running as a workflow task

# COMMAND ----------

dbutils.notebook.exit("End of notebook when running as a workflow task")

# COMMAND ----------

# MAGIC %md
# MAGIC # Exercise
# MAGIC Go check out the lineage of the table produced by Unity Catalog inside the Data Explorer
# MAGIC 1. Click on the `Data` tab in the left panel
# MAGIC 2. Find your table
# MAGIC 3. Check out the lineage table and graph for both `Tables` and `Notebooks`
