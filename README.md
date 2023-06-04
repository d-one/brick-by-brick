# sds-brick-by-brick
Repository for the SDS databricks brick-by-brick workshop

# Content
1. Setup Workspace
    * Getting Accesss
    * Setup Repos

2. Data Engineering 
    * Upload the data through the workspace
    * Processing the data with PySpark notebooks using the medallion architecture.
    * Save the data in the Unity Catalog as Delta tables.
    * Create a data pipeline using databricks workflows.

# Setup Workspace
Login to the [workspace](https://adb-3967117302852551.11.azuredatabricks.net/?o=3967117302852551) using the email address you used to sign-up for the SDS workshop

## Adding the repository
Adding the repository to your workspace: 
   * Click on `Repos` in the Menu to the left
   * Click on the directory with your email address.
   * Click on `Add Repo` and paste this [URL](https://github.com/d-one/sds-brick-by-brick) into `Git repository` 

## Create a personal cluster to your workspace.
1. Click on the Compute tab in the left bar
2. Click on Create compute and choose the following settings:
3. Choose the `sds-compute-policy` Policy
3. Make sure the `Single user access` is under your name
4. Click on `Create Cluster`

# Building a Data Pipeline in Databricks
## Configuring the Notebooks
1. Go to the notebook `Bronze` and follow the instructions. 
2. Go to the notebook `Silver` and follow the instructions. 
3. Go to the notebook `Gold` and follow the instructions. 