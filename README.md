# brick-by-brick
Repository for the D ONE databricks brick-by-brick workshop

# Content
1. Setup Workspace
    * Adding the repository
    * Create a personal cluster

2. Delta + Unity Catalog
   * Read and Write Tables
   * Upload data to Unity Catalog
   * Time Travel + Installing Libraries

3. Medallion Architecture & Workflow Orchestration
   * 3 Notebooks - Medallion architecture 
   * Creating a Workflow Job


# 1. Setup Workspace
Login to the [workspace](https://adb-1451829595406012.12.azuredatabricks.net/?o=1451829595406012#) using the email address you used to sign-up for the workshop.

## Adding the repository
Adding the repository to your workspace: 
1. Click on `Workspace` in the navigation menu to the left.
2. Click on the directory `Repos` and then on the directory with your email address (If it does not exist, create a new directory named like your email address).
3. Click on `Add Repo` and paste this [URL](https://github.com/d-one/brick-by-brick) into `Git repository` 
4. Click on `Create Repo` 

Now you should see a repository named `sds-brick-by-brick` under your own directory.

## Create a personal cluster to your workspace.
1. Click on the `Compute` tab in the navigation menu to the left.
2. Click on Create compute and choose the following settings:
3. Choose the `sds-compute-policy` Policy
3. Make sure the `Single user access` is under your name
4. Click on `Create Cluster`

# 2. 3 Notebooks - the medallion architecture
Go to the following notebooks and follow the instructions:
1. `Bronze`. 
2. `Silver` 
3. `Gold`

# 3. Creating a Workflow Job
1. Click on the `Workflows` tab in the navigation menu to the left.
2. Click on the `Create job` button.
3. Add a Job name for your Workflow at the top: `my_medallion_job_<firstname>_<lastname>`.
3. Choose the following settings
   * **Task Name**: `bronze_task`
   * **Source**: `Workspace`
   * **Path**: Click on `Select Notebook` and choose your Bronze Notebook
   * **Cluster**: Choose your existing All-Purpose-Cluster you created in your first exercise. 
4. Click on `Create`
   * Now you have created a workflow Job with one task inside.
5. Click on `Add task` and choose `Notebook`
6. Repeat the steps for both the `silver_task`and `gold_task`. 
   * Make sure that they are dependent on each other in the following order *bronze_task -> silver_task -> gold_task*
7. Click on `Run now` to run the whole Job.

Congratulations, you have now created a workflow Job.


# 4. ML and MLOps

1. Run the ML Preprocessing notebook in your catalog to create the feature table.
2. Move on to the ML MLflow Tracking notebook and walk through the steps to understand how to interact with MLflow experiments inside the Databricks workspace.
3. Move on to the ML Model Registry notebook and walk through the steps to understand how to interact with the model registry via python APIs or via the directly using the UI
4. (Optional)Tie steps 1-3 together by creating a new ML workflow! See the results of the workflow run in the UI.
5. (Optional)Finally move on to the AutoML notebook and see for yourself how easy it is to use databricks AutoML as a quick way to create baseline models.
  

Excellent, you have now mastered MLflow on Databricks and you are ready to apply these principals to your own project.


