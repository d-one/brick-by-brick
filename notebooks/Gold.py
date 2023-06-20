# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # Read a table from Unity Catalog
# MAGIC Namespace
# MAGIC * Catalog = sds_catalog
# MAGIC * Schema = default
# MAGIC * Table = laptop_prices

# COMMAND ----------

df_silver = spark.table("robert_yousif.silver.laptop_prices")
display(df_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Make some aggregations 

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

df_laptop_gold_1 = spark.sql(
    """
    SELECT
        Company,
        count(Company) as num_of_laptops,
        AVG(Price_euros) as avg_price
    FROM
        robert_yousif.silver.laptop_prices
    GROUP BY
        Company
    ORDER BY
        num_of_laptops DESC                   
    """
)

# COMMAND ----------

# MAGIC %md 
# MAGIC # Writing the dataframe to Unity Catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Your Gold Schema
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS robert_yousif.gold

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Save the dataframe as a delta table inside the unity catalog

# COMMAND ----------

catalog_name = "robert_yousif"
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
