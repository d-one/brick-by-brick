# Databricks notebook source
# MAGIC %md
# MAGIC # Read a table from Unity Catalog
# MAGIC In this notebook we will do some aggregations

# COMMAND ----------

# ********* workflow parameters ********* #
# set parameters here only if running notebook, for example:
# dbutils.widgets.text("CATALOG_NAME", "uat_scratch_kni")

# COMMAND ----------

# set up catalog name either by workflow parameters or by using current user's id
try:
    catalog_name = dbutils.widgets.get("CATALOG_NAME")
except:
    user_email = spark.sql('select current_user() as user').collect()[0]['user']
    catalog_name = user_email.split('@')[0].replace(".", "_").replace("-", "_")

# COMMAND ----------

df_silver = spark.table(f"{catalog_name}.silver.churn_modelling")
display(df_silver.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Make some aggregations 

# COMMAND ----------

# MAGIC %md
# MAGIC You can write it as pure SQL or embedded SQL. You can also do the aggregations using pure pyspark.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   Geography,
# MAGIC   count(CustomerId) as num_customers,
# MAGIC   AVG(Balance) as avg_balance,
# MAGIC   AVG(Age) as avg_age
# MAGIC FROM
# MAGIC   panagiotis_goumenakis.silver.churn_modelling
# MAGIC GROUP BY
# MAGIC   Geography
# MAGIC ORDER BY
# MAGIC   num_customers DESC

# COMMAND ----------

df_churn_gold_1 = spark.sql(
    f"""
    SELECT
        Geography,
        count(CustomerId) as num_customers,
        AVG(Balance) as avg_balance
    FROM
        {catalog_name}.silver.churn_modelling
    GROUP BY
        Geography
    ORDER BY
        num_customers DESC                   
    """
)
display(df_churn_gold_1)

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
table_name = "churn_modelling_1"

df_churn_gold_1.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog_name}.{schema_name}.{table_name}")


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
