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

MODEL_VERSION_URI = "https://adb-1451829595406012.12.azuredatabricks.net/serving-endpoints/gtc_serving_demo/invocations"
DATABRICKS_API_TOKEN = dbutils.secrets.get(scope="bricks", key="token")

# COMMAND ----------

# data sample to send to the model endpoint
data =  pd.DataFrame([{
    'screen_size_norm': 15,	
    'total_pixels': 2073600,
    'ram_size': 32,
    'weight_kg': 5,
    'memory_size': 500,
    'cpu_clock_spead': 4,
    'Company_Acer': 0,
    'Company_Apple':  0,
    'Company_Asus': 0,
    'Company_Chuwi':  0,
    'Company_Dell': 1,
    'Company_Fujitsu':  0,
    'Company_Google': 0,
    'Company_HP': 0,
    'Company_Huawei': 0,
    'Company_LG': 0,
    'Company_Lenovo':  0,
    'Company_MSI': 0,
    'Company_Mediacom': 0,
    'Company_Microsoft': 0,
    'Company_Razer': 0,
    'Company_Samsung': 0,
    'Company_Toshiba':  0,
    'Company_Vero': 0,
    'Company_Xiaomi': 0,
    'TypeName_2 in 1 Convertible': 0,
    'TypeName_Gaming': 0,
    'TypeName_Netbook': 0,
    'TypeName_Notebook':  0,
    'TypeName_Ultrabook': 1,
    'TypeName_Workstation':  0,
    'operating_system_linux':  0,
    'operating_system_mac': 0,
    'operating_system_no os':  0,
    'operating_system_other': 0,
    'operating_system_windows': 1,
    'memory_type_hdd': 0,
    'memory_type_hybrid':  0,
    'memory_type_ssd': 1,
    'cpu_manufacturer_amd':  0,
    'cpu_manufacturer_intel': 1,
    'cpu_manufacturer_other':  0,
    'gpu_manufacturer_amd':  0,
    'gpu_manufacturer_intel': 1,
    'gpu_manufacturer_nvidia': 0,
    'gpu_manufacturer_other': 0
}])

# COMMAND ----------

# Scoring a model that accepts pandas DataFrames

score_model(MODEL_VERSION_URI, DATABRICKS_API_TOKEN, data)
