# Databricks notebook source
# MAGIC  %md-sandbox
# MAGIC
# MAGIC <div style="text-align: left; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://s3.eu-central-1.amazonaws.com/co.lever.eu.client-logos/c2f22a4d-adbd-49a9-a9ca-c24c0bd5dc1a-1607101144408.png" alt="D ONE" style="width: 300px">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # Pre processing
# MAGIC
# MAGIC In any data science project, feaure engineering is very important  seeks to address these folowing points:
# MAGIC
# MAGIC * Analyze your data and figure out what to use from them 
# MAGIC * Define and extract a set of features that represent data that's important for the analysis (feature extraction)
# MAGIC * Transform a particular set of input features to make a new set of more effective features (feature construction)
# MAGIC
# MAGIC In this notebook, we will do some basic analysis of our data and finalize a set of features to be used in our ML modeling.

# COMMAND ----------

import pandas as pd
import numpy as np
import re

# COMMAND ----------

# user parameters
user_email = spark.sql('select current_user() as user').collect()[0]['user']
user_name = user_email.split('@')[0].replace(".", "_").replace("-", "_")
catalog_name = "placeholder_catalog"

# COMMAND ----------

path = f"file:/Workspace/Repos/{user_email}/brick-by-brick/data/churn_modelling.csv"

dbutils.fs.ls(path)

# COMMAND ----------

try:
    sdf_raw = spark.read.format("csv").option("header", "true").load(path)
except:
    print("File does not exist, please make sure that your path is correct and that you have pulled the repository to databricks repos")

# COMMAND ----------

# write data

catalog_name = "placeholder_catalog"
schema_name = "placeholder_schema"
table_name = f"churn_modelling_{user_name}"

sdf_raw.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.{table_name}")


# COMMAND ----------

# What are the attributes?
sdf_raw.printSchema()
# Let's have a look at the first couple of records
sdf_raw.limit(5).display()

# COMMAND ----------

#check if all id unique. 
print(sdf_raw.select("CustomerId").distinct().count() == sdf_raw.count())
#print n_rows
print(f"n_rows in dataset: {sdf_raw.count()}")

# COMMAND ----------

# Dataset is pretty small, switching to Pandas
df = sdf_raw.toPandas()

# COMMAND ----------

df.info()

# COMMAND ----------

float_columns = ['Balance', 'EstimatedSalary']
integer_columns = ['CreditScore', 'Age', 'Tenure', 'NumOfProducts', 'HasCrCard', 'IsActiveMember', 'Exited']

for column in float_columns:
    df[column] = df[column].astype('float')
for column in integer_columns:
    df[column] = df[column].astype('int64')

# COMMAND ----------

# count number of cases in target variable
df['Exited'].value_counts()

# COMMAND ----------

#obtain statistics for nominal features
df.describe(include=['object'])

# COMMAND ----------

# generate features
generated_features = ['IsActive_by_CreditCard', 'Products_Per_Tenure', 'ZeroBalance', 'AgeCat']

df['IsActive_by_CreditCard'] = df['HasCrCard'] * df['IsActiveMember']
df['Products_Per_Tenure'] =  df['Tenure'] / df['NumOfProducts']
df['ZeroBalance'] = (df['Balance'] == 0).astype('int64')
df['AgeCat'] = np.round(df['Age']/20).astype('int64')

# COMMAND ----------

# pick features
columns = float_columns + integer_columns + generated_features + ['Geography', 'Gender']
features_df = df[columns]

# COMMAND ----------

features_sdf =spark.createDataFrame(features_df) 
features_sdf.printSchema()

# COMMAND ----------

target_table_name = f"features_{user_name}"

features_sdf.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.{target_schema_name}.{target_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Conclusion
# MAGIC * In this notebook you feature engineered your dataset for the upcoming ML task
# MAGIC
# MAGIC **Next:** Go to the MLflow Tracking notebook and continue from there
