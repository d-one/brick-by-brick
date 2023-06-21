# Databricks notebook source
# MAGIC  %md-sandbox
# MAGIC
# MAGIC <div style="text-align: left; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://s3.eu-central-1.amazonaws.com/co.lever.eu.client-logos/c2f22a4d-adbd-49a9-a9ca-c24c0bd5dc1a-1607101144408.png" alt="D ONE" style="width: 300px">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # AutoML
# MAGIC
# MAGIC <a href="https://docs.databricks.com/applications/machine-learning/automl.html" target="_blank">Databricks AutoML</a> helps you automatically build machine learning models both through a UI and programmatically. It prepares the dataset for model training and then performs and records a set of trials (using HyperOpt), creating, tuning, and evaluating multiple models. 
# MAGIC
# MAGIC In this notebook you will:
# MAGIC  - Use AutoML to automatically train and tune your models
# MAGIC  - Run AutoML in Python and through the UI
# MAGIC  - Interpret the results of an AutoML run

# COMMAND ----------

# MAGIC %md <i18n value="7aa84cf3-1b6c-4ba4-9249-00359ee8d70a"/>
# MAGIC
# MAGIC
# MAGIC
# MAGIC Currently, AutoML uses a combination of XGBoost and sklearn (only single node models) but optimizes the hyperparameters within each.

# COMMAND ----------

# set up the below params
user_email = "spyros.cavadias@ms.d-one.ai"
user_name = "spyros_cavadias"

# COMMAND ----------

catalog_name = user_name
schema_name = "silver"
table_name = "features"

df = spark.read.table(f"{catalog_name}.{schema_name}.{table_name}").toPandas()

# COMMAND ----------

from sklearn.model_selection import train_test_split
import pandas as pd

# one-hot encode categorical columns 
#categorical data
categorical_cols = ['Company', 'TypeName', 'operating_system','memory_type','cpu_manufacturer','gpu_manufacturer']
# get_dummies
enriched_df = pd.get_dummies(df, columns = categorical_cols)

# train test split 
train_df, test_df, _, _ = train_test_split(enriched_df, enriched_df[["Price_euros"]].values.ravel(), test_size=0.1, random_state=42)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC We can now use AutoML to search for the optimal <a href="https://docs.databricks.com/applications/machine-learning/automl.html#regression" target="_blank">regression</a> model. 
# MAGIC
# MAGIC Required parameters:
# MAGIC * **`dataset`** - Input Spark or pandas DataFrame that contains training features and targets. If using a Spark DataFrame, it will convert it to a Pandas DataFrame under the hood by calling .toPandas() - just be careful you don't OOM!
# MAGIC * **`target_col`** - Column name of the target labels
# MAGIC
# MAGIC We will also specify these optional parameters:
# MAGIC * **`primary_metric`** - Primary metric to select the best model. Each trial will compute several metrics, but this one determines which model is selected from all the trials. One of **`r2`** (default, R squared), **`mse`** (mean squared error), **`rmse`** (root mean squared error), **`mae`** (mean absolute error) for regression problems.
# MAGIC * **`timeout_minutes`** - The maximum time to wait for the AutoML trials to complete. **`timeout_minutes=None`** will run the trials without any timeout restrictions
# MAGIC * **`max_trials`** - The maximum number of trials to run. When **`max_trials=None`**, maximum number of trials will run to completion.

# COMMAND ----------

from databricks import automl

summary = automl.regress(train_df, target_col="Price_euros", primary_metric="rmse", timeout_minutes=5, max_trials=10)

# COMMAND ----------

# MAGIC %md 
# MAGIC  
# MAGIC
# MAGIC After running the previous cell, you will notice two notebooks and an MLflow experiment:
# MAGIC * **`Data exploration notebook`** - we can see a Profiling Report which organizes the input columns and discusses values, frequency and other information
# MAGIC * **`Best trial notebook`** - shows the source code for reproducing the best trial conducted by AutoML
# MAGIC * **`MLflow experiment`** - contains high level information, such as the root artifact location, experiment ID, and experiment tags. The list of trials contains detailed summaries of each trial, such as the notebook and model location, training parameters, and overall metrics.
# MAGIC
# MAGIC Dig into these notebooks and the MLflow experiment - what do you find?
# MAGIC
# MAGIC Additionally, AutoML shows a short list of metrics from the best run of the model.

# COMMAND ----------

print(summary.best_trial)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC Now we can test the model that we got from AutoML against our test data. We'll be using <a href="https://mlflow.org/docs/latest/python_api/mlflow.pyfunc.html#mlflow.pyfunc.spark_udf" target="_blank">mlflow.pyfunc.spark_udf</a> to register our model as a UDF and apply it in parallel to our test data.

# COMMAND ----------

# Load the best trial as an MLflow Model
import mlflow

model_uri = f"runs:/{summary.best_trial.mlflow_run_id}/model"

predict = mlflow.pyfunc.spark_udf(spark, model_uri)
test_sdf = spark.createDataFrame(test_df)
pred_sdf = test_sdf.withColumn("prediction", predict(*test_sdf.drop("Price_euros").columns))
display(pred_sdf)

# COMMAND ----------

from pyspark.ml.evaluation import RegressionEvaluator

regression_evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="Price_euros", metricName="rmse")
rmse = regression_evaluator.evaluate(pred_sdf)
print(f"RMSE on test dataset: {rmse:.3f}")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
