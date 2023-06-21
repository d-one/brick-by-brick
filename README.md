# sds-brick-by-brick
Repository for the SDS databricks brick-by-brick workshop

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


# Setup Workspace
Login to the [workspace](https://adb-3967117302852551.11.azuredatabricks.net/?o=3967117302852551) using the email address you used to sign-up for the SDS workshop

## Adding the repository
Adding the repository to your workspace: 
1. Click on `Repos` in the navigation menu to the left.
2. Click on the directory with your email address.
3. Click on `Add Repo` and paste this [URL](https://github.com/d-one/sds-brick-by-brick) into `Git repository` 
4. Click on `Create Repo` 

Now you should see a repository named `sds-brick-by-brick` under your own directory.

## Create a personal cluster to your workspace.
1. Click on the `Compute` tab in the navigation menu to the left.
2. Click on Create compute and choose the following settings:
3. Choose the `sds-compute-policy` Policy
3. Make sure the `Single user access` is under your name
4. Click on `Create Cluster`

# 3 Notebooks - the medallion architecture
Go to the following notebooks and follow the instructions:
1. `Bronze`. 
2. `Silver` 
3. `Gold`

# Creating a Workflow Job
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


