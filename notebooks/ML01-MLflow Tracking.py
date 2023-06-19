# Databricks notebook source
# MAGIC  %md-sandbox
# MAGIC
# MAGIC <div style="text-align: left; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://s3.eu-central-1.amazonaws.com/co.lever.eu.client-logos/c2f22a4d-adbd-49a9-a9ca-c24c0bd5dc1a-1607101144408.png" alt="D ONE" style="width: 400px">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md <i18n value="b27f81af-5fb6-4526-b531-e438c0fda55e"/>
# MAGIC
# MAGIC
# MAGIC
# MAGIC # MLflow
# MAGIC
# MAGIC <a href="https://mlflow.org/docs/latest/concepts.html" target="_blank">MLflow</a> seeks to address these three core issues:
# MAGIC
# MAGIC * It’s difficult to keep track of experiments
# MAGIC * It’s difficult to reproduce code
# MAGIC * There’s no standard way to package and deploy models
# MAGIC
# MAGIC In the past, when examining a problem, you would have to manually keep track of the many models you created, as well as their associated parameters and metrics. This can quickly become tedious and take up valuable time, which is where MLflow comes in.
# MAGIC
# MAGIC MLflow is pre-installed on the Databricks Runtime for ML.

# COMMAND ----------

# MAGIC %md <i18n value="b7c8a0e0-649e-4814-8310-ae6225a57489"/>
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/mlflow-tracking.png" style="height: 400px; margin: 20px"/></div>

# COMMAND ----------

# MAGIC %md <i18n value="c1a29688-f50a-48cf-9163-ebcc381dfe38"/>
# MAGIC
# MAGIC
# MAGIC
# MAGIC Let's start by loading in our dataset.

# COMMAND ----------

catalog_name = "spyros_cavadias"
schema_name = "silver"
table_name = "features"

df = spark.read.table(f"{catalog_name}.{schema_name}.{table_name}").toPandas()

# COMMAND ----------

# MAGIC %md <i18n value="9ab8c080-9012-4f38-8b01-3846c1531a80"/>
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### MLflow Tracking
# MAGIC
# MAGIC MLflow Tracking is a logging API specific for machine learning and agnostic to libraries and environments that do the training.  It is organized around the concept of **runs**, which are executions of data science code.  Runs are aggregated into **experiments** where many runs can be a part of a given experiment and an MLflow server can host many experiments.
# MAGIC
# MAGIC You can use <a href="https://mlflow.org/docs/latest/python_api/mlflow.html#mlflow.set_experiment" target="_blank">mlflow.set_experiment()</a> to set an experiment, but if you do not specify an experiment, it will automatically be scoped to this notebook.

# COMMAND ----------

# MAGIC %md <i18n value="82786653-4926-4790-b867-c8ccb208b451"/>
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### Track Runs
# MAGIC
# MAGIC Each run can record the following information:<br><br>
# MAGIC
# MAGIC - **Parameters:** Key-value pairs of input parameters such as the number of trees in a random forest model
# MAGIC - **Metrics:** Evaluation metrics such as RMSE or Area Under the ROC Curve
# MAGIC - **Artifacts:** Arbitrary output files in any format.  This can include images, pickled models, and data files
# MAGIC - **Source:** The code that originally ran the experiment
# MAGIC
# MAGIC **NOTE**: For Spark models, MLflow can only log PipelineModels.

# COMMAND ----------

import mlflow
import mlflow.sklearn
from mlflow.models.signature import infer_signature

import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from math import sqrt, log, exp

# COMMAND ----------

# one-hot encode categorical columns 
#categorical data
categorical_cols = ['Company', 'TypeName', 'operating_system','memory_type','cpu_manufacturer','gpu_manufacturer']
# get_dummies
enriched_df = pd.get_dummies(df, columns = categorical_cols)

# train test split 
X_train, X_test, y_train, y_test = train_test_split(enriched_df.drop(["Price_euros"], axis=1), enriched_df[["Price_euros"]].values.ravel(), test_size=0.1, random_state=42)


# COMMAND ----------

# set experiment name 
email = "spyros.cavadias@ms.d-one.ai"
experiment = mlflow.set_experiment(f"/Users/{email}/sds_mlflow_experiment")

# COMMAND ----------

with mlflow.start_run(run_name="LR-Numerical-Features") as run:
    # Define pipeline
    lr = LinearRegression()
    features = ["screen_size_norm",	"total_pixels","ram_size","weight_kg","memory_size","cpu_clock_spead"]
    model = lr.fit(X_train[features], y_train)

    # Log parameters
    mlflow.log_param("label", "price")
    mlflow.log_param("features", features)

    # Log model
    mlflow.sklearn.log_model(model, "model", input_example=X_train[features].head(5)) 

    # Evaluate predictions
    pred = model.predict(X_test[features])
    rmse = sqrt(mean_squared_error(y_test, pred))

    # Log metrics
    mlflow.log_metric("rmse", rmse)

# COMMAND ----------

# MAGIC %md <i18n value="44bc7cac-de4a-47e7-bfff-6d2eb58172cd"/>
# MAGIC
# MAGIC
# MAGIC
# MAGIC There, all done! Let's go through the other two linear regression models and then compare our runs. 
# MAGIC
# MAGIC **Question**: Does anyone remember the RMSE of the other runs?
# MAGIC
# MAGIC Next let's build our linear regression model but use all of our features.

# COMMAND ----------


with mlflow.start_run(run_name="LR-All-Features") as run:
    # Define pipeline
    lr = LinearRegression()
    model = lr.fit(X_train, y_train)

    # Log parameters
    mlflow.log_param("label", "price")
    mlflow.log_param("features", "all_features")

    # Log model
    mlflow.sklearn.log_model(model, "model", input_example=X_train.head(5)) 

    # Evaluate predictions
    pred = model.predict(X_test)
    rmse = sqrt(mean_squared_error(y_test, pred))
    r2 = r2_score(y_test, pred)

    # Log metrics
    mlflow.log_metric("rmse", rmse)
    mlflow.log_metric("r2", r2)

# COMMAND ----------

# MAGIC %md <i18n value="70188282-8d26-427d-b374-954e9a058000"/>
# MAGIC
# MAGIC
# MAGIC
# MAGIC Finally, we will use Linear Regression to predict the log of the price, due to its log normal distribution. 
# MAGIC
# MAGIC We'll also practice logging artifacts to keep a visual of our log normal histogram.

# COMMAND ----------

import matplotlib.pyplot as plt

with mlflow.start_run(run_name="LR-Log-Price") as run:
    lr = LinearRegression()
    y_train_log = [log(x) for x in y_train]
    model = lr.fit(X_train, y_train_log)

    # Log parameters
    mlflow.log_param("label", "log_price")
    mlflow.log_param("features", "all_features")

    # Log model
    mlflow.sklearn.log_model(model, "model", input_example=X_train.head(5)) 

    # Evaluate predictions
    log_pred = model.predict(X_test)
    pred = [exp(x) for x in log_pred]
    rmse = sqrt(mean_squared_error(y_test, pred))
    r2 = r2_score(y_test, pred)

    # Log metrics
    mlflow.log_metric("rmse", rmse)
    mlflow.log_metric("r2", r2)

    # Log artifact
    plt.clf()

    pd.DataFrame(y_train_log, columns = ["log_price"]).hist(column="log_price", bins=100)
    fig = plt.gcf()
    mlflow.log_figure(fig, "log_normal.png")
    plt.show()

# COMMAND ----------

# MAGIC %md <i18n value="66785d5e-e1a7-4896-a8a9-5bfcd18acc5c"/>
# MAGIC
# MAGIC
# MAGIC
# MAGIC That's it! Now, let's use MLflow to easily look over our work and compare model performance. You can either query past runs programmatically or use the MLflow UI.

# COMMAND ----------

# MAGIC %md <i18n value="0b1a68e1-bd5d-4f78-a452-90c7ebcdef39"/>
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### Querying Past Runs
# MAGIC
# MAGIC You can query past runs programmatically in order to use this data back in Python.  The pathway to doing this is an **`MlflowClient`** object.

# COMMAND ----------

from mlflow.tracking import MlflowClient

client = MlflowClient()

# COMMAND ----------

# MAGIC %md <i18n value="dcd771b2-d4ed-4e9c-81e5-5a3f8380981f"/>
# MAGIC
# MAGIC
# MAGIC
# MAGIC You can also use <a href="https://mlflow.org/docs/latest/search-syntax.html" target="_blank">search_runs</a> to find all runs for a given experiment.

# COMMAND ----------

experiment_id = run.info.experiment_id
runs_df = mlflow.search_runs(experiment_id)

display(runs_df)

# COMMAND ----------

# MAGIC %md <i18n value="68990866-b084-40c1-beee-5c747a36b918"/>
# MAGIC
# MAGIC
# MAGIC
# MAGIC Pull the last run and look at metrics.

# COMMAND ----------

runs = client.search_runs(experiment_id, order_by=["attributes.start_time desc"], max_results=1)
runs[0].data.metrics

# COMMAND ----------

runs[0].info.run_id

# COMMAND ----------

# MAGIC %md <i18n value="cfbbd060-6380-444f-ba88-248e10a56559"/>
# MAGIC
# MAGIC
# MAGIC
# MAGIC Examine the results in the UI.  Look for the following:<br><br>
# MAGIC
# MAGIC 1. The **`Experiment ID`**
# MAGIC 2. The artifact location.  This is where the artifacts are stored in DBFS.
# MAGIC 3. The time the run was executed.  **Click this to see more information on the run.**
# MAGIC 4. The code that executed the run.
# MAGIC
# MAGIC
# MAGIC After clicking on the time of the run, take a look at the following:<br><br>
# MAGIC
# MAGIC 1. The Run ID will match what we printed above
# MAGIC 2. The model that we saved, included a pickled version of the model as well as the Conda environment and the **`MLmodel`** file.
# MAGIC
# MAGIC Note that you can add notes under the "Notes" tab to help keep track of important information about your models. 
# MAGIC
# MAGIC Also, click on the run for the log normal distribution and see that the histogram is saved in "Artifacts".

# COMMAND ----------

# MAGIC %md <i18n value="63ca7584-2a86-421b-a57e-13d48db8a75d"/>
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### Load Saved Model
# MAGIC
# MAGIC Let's practice <a href="https://www.mlflow.org/docs/latest/python_api/mlflow.spark.html" target="_blank">loading</a> our logged log-normal model.

# COMMAND ----------

model_path = f"runs:/{run.info.run_id}/model"
loaded_model = mlflow.sklearn.load_model(model_path)

display(loaded_model.predict(X_test))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
