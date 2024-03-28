# Databricks notebook source
# MAGIC  %md-sandbox
# MAGIC
# MAGIC <div style="text-align: left; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://s3.eu-central-1.amazonaws.com/co.lever.eu.client-logos/c2f22a4d-adbd-49a9-a9ca-c24c0bd5dc1a-1607101144408.png" alt="D ONE" style="width: 300px">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Model Serving
# MAGIC
# MAGIC <a href="https://docs.databricks.com/machine-learning/model-serving/index.html" target="_blank">Model serving</a> exposes your MLflow machine learning models as scalable REST API endpoints and provides a highly available and low-latency service for deploying models. The service automatically scales up or down to meet demand changes within the chosen concurrency range.
# MAGIC
# MAGIC In this notebook you can directly sent post requests to a deployed model endpoint!
# MAGIC

# COMMAND ----------

import numpy as np
import pandas as pd
import requests
import json

# COMMAND ----------

def score_model(model_uri, databricks_token, data):
  headers = {
    "Authorization": f"Bearer {databricks_token}",
    "Content-Type": "application/json",
  }
  _dict = data.to_dict(orient='split')
  del _dict["index"]
  data_json = {'dataframe_split': _dict}
  response = requests.request(method='POST', headers=headers, url=model_uri, json=data_json)
  if response.status_code != 200:
      raise Exception(f"Request failed with status {response.status_code}, {response.text}")
  return response.json()

# COMMAND ----------

# MAGIC %md
# MAGIC  
# MAGIC  In order to interact with the served model, you need to also provide the model uri and a databricks api token. These are already provided for you.
# MAGIC
# MAGIC  

# COMMAND ----------

MODEL_VERSION_URI = "https://adb-1451829595406012.12.azuredatabricks.net/serving-endpoints/opap_serving_demo/invocations"
DATABRICKS_API_TOKEN = ""

# COMMAND ----------

# data sample to send to the model endpoint
data =  pd.DataFrame([{
    'Balance': 114753.76,
    'EstimatedSalary': 107665.02,
    'CreditScore': 691.0,
    'Age': 38.0,
    'Tenure': 5.0,
    'NumOfProducts': 1.0,
    'HasCrCard': 1.0,
    'IsActiveMember': 0.0,
    'Exited': 0.0,
    'IsActive_by_CreditCard': 0.0,
    'Products_Per_Tenure': 5.0,
    'ZeroBalance': 0.0,
    'AgeCat': 2.0,
    'Geography_France': 0.0,
    'Geography_Germany': 1.0,
    'Geography_Spain': 0.0,
    'Gender_Female': 1.0,
    'Gender_Male': 0.0,
}])

# COMMAND ----------

# Scoring a model that accepts pandas DataFrames

score_model(MODEL_VERSION_URI, DATABRICKS_API_TOKEN, data)
