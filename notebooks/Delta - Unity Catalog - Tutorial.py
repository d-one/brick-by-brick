# Databricks notebook source
# MAGIC %md
# MAGIC # Read a table from Unity Catalog
# MAGIC Namespace
# MAGIC * Catalog = sds_catalog
# MAGIC * Schema = default
# MAGIC * Table = laptop_data

# COMMAND ----------

df = spark.table("gtc_catalog.default.laptop_prices_euro")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Using SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM gtc_catalog.default.laptop_prices_euro

# COMMAND ----------

# MAGIC %md
# MAGIC Using embedded SQL

# COMMAND ----------

df = spark.sql("SELECT * FROM gtc_catalog.default.laptop_prices_euro")

# COMMAND ----------

# MAGIC %md
# MAGIC # Save and read temporary views

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temptable_sql_laptop_data
# MAGIC AS
# MAGIC SELECT * FROM gtc_catalog.default.laptop_prices_euro

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM temptable_sql_laptop_data

# COMMAND ----------

df = spark.table("temptable_sql_laptop_data")
display(df)

# COMMAND ----------

# MAGIC %md 
# MAGIC Use Python to create a temporary view

# COMMAND ----------

df = spark.table("gtc_catalog.default.laptop_prices_euro")
df.createOrReplaceTempView("temptable_python_laptop_data")


# COMMAND ----------

# test with new column and append

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VIEWS

# COMMAND ----------

# MAGIC %md
# MAGIC # Delta table history

# COMMAND ----------


df = spark.table("gtc_catalog.default.laptop_prices_euro")
df.write.format("delta").mode("append").saveAsTable("gtc_catalog.default.laptop_prices_euro")


# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY gtc_catalog.default.laptop_prices_euro

# COMMAND ----------

df_v1 = spark.table("gtc_catalog.default.laptop_prices_euro@v0")
print(df_v1.count())

df_v2 = spark.table("gtc_catalog.default.laptop_prices_euro@v1")
print(df_v2.count())

# COMMAND ----------

# MAGIC %md 
# MAGIC Using SQL to read an older version

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM gtc_catalog.default.laptop_prices_euro VERSION AS OF 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Show the `Variable Explorer`, `Revision History` & `Python Libraries`

# COMMAND ----------

# MAGIC %md
# MAGIC # Exercises

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1 

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Download the data from the repository: [laptop_data](https://github.com/d-one/sds-brick-by-brick/tree/main/data)
# MAGIC 2. Upload the data as a workspace object to your personal directory.
# MAGIC 3. Read the data and display it
# MAGIC 4. Write to your own table inside your own catalog and schema
# MAGIC 5. See what other `DESCRIBE` commands you can run on your table to get more information
# MAGIC 6. Share the table with the person sitting next to you. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Download the data from the repository
# MAGIC Click on this [link](https://github.com/d-one/sds-brick-by-brick/blob/main/data/laptop_data.csv) and download the data directly from github to your local machine. (You will find a `Download Raw File` Button at the top right of the document preview inside gitlab.)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Upload Workspace Objects
# MAGIC
# MAGIC 1. Click on the `Workspace` button in the panel, this will take you to another page (Open it in a new page to keep the intstruction up).
# MAGIC     * `Home`: Private directories for every user.
# MAGIC     * `Workspace`: Shared and Users directories.
# MAGIC     * `Repos`: All repositories.
# MAGIC
# MAGIC 2. Click on the `Workspace` -> `Users` and choose your name, if your name does not exist, create a directory with your name `firstname_lastname`. 
# MAGIC 3. Right click inside your directory and click on `Import`, this will open a new window with the name `Import`
# MAGIC 4. Choose File and either drop a file or browse for it. 
# MAGIC 5. Click on the `Import` button

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 3. Read the data and display it
# MAGIC TIP: Use the path `"file:/Workspace/Users/<firstname>_<lastname>/<path-to-file>"`

# COMMAND ----------

user_email = "spyros.cavadias@ms.d-one.ai"
path = f"file:/Workspace/Users/{user_email}/laptop_price_euro.csv"
# <TODO>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Write to your own table inside your own catalog and the default schema

# COMMAND ----------

catalog_name = "spyros.cavadias@ms.d-one.ai"
table_name = "my_uploaded_laptop_price_table"
# <TODO>


display(spark.table(f"{catalog_name}.default.{table_name}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. See what other `DESCRIBE` commands you can run on your table to get more information
# MAGIC Hint: `DESCRIBE DETAIL`, `DESCRIBE HISTORY`
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- <TODO>
# MAGIC GRANT SELECT on CATALOG 

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Share the table with the person sitting next to you so they can read it from your catalog
# MAGIC This can be done either through the GUI or SQL commands such as: 
# MAGIC `GRANT <permission> ON <object> <objectname>` for example `GRANT SELECT ON TABLE sds_catalog.default.laptop_data`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Either do it here or in the GUI

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solution

# COMMAND ----------

# Make sure you have uploaded the file
path = f"file:/Workspace/Users/{user_email}/laptop_price_euro.csv"
dbutils.fs.ls(path)

# COMMAND ----------

df = spark.read.format("csv").option("header", "true").load(path)
display(df)
df.write.format("delta").mode("append").saveAsTable(f"{catalog_name}.default.{table_name}")
#

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL robert_yousif.default.my_uploaded_laptop_price_table

# COMMAND ----------

# MAGIC %md 
# MAGIC # Exercise Extended 

# COMMAND ----------

# MAGIC %md
# MAGIC Install a library
# MAGIC * Install a Cluster library
# MAGIC * Install a Notebook-scoped library
# MAGIC
# MAGIC Follow the instructions here: Installing Libraries
# MAGIC
# MAGIC
# MAGIC Check the `Python Libraries` tab on the right panel or run a `pip freeze`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cluster Library
# MAGIC
# MAGIC A cluster library will be available to all the Notebooks that are connected to that cluster.
# MAGIC
# MAGIC 1. Click compute icon `Compute` in the sidebar.
# MAGIC 2. Click a cluster name.
# MAGIC 3. Click the `Libraries` tab.
# MAGIC 4. Click `Install New`.
# MAGIC 5. Choose one of the `Library Source` buttons
# MAGIC     * You have a few options here i.e. `Upload` or `PyPI`
# MAGIC     * Every time you start the cluster, the libraries will be installed again

# COMMAND ----------

# MAGIC %md
# MAGIC ### Notebook-scoped Libraries
# MAGIC When you install a notebook-scoped library, only the current notebook and any jobs associated with that notebook have access to that library. Other notebooks attached to the same cluster are not affected.
# MAGIC
# MAGIC To install a library in a notebook scoped:
# MAGIC
# MAGIC ```sh
# MAGIC %pip install arrow
# MAGIC ```

# COMMAND ----------

# MAGIC %pip install arrow

# COMMAND ----------

# MAGIC %md
# MAGIC TIP: Try `Detach & re-attach`your cluster and see if the library is still installed
