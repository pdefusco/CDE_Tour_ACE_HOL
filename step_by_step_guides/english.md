## Requirements

In order to execute the Hands On Labs you need:
* A CDE Virtual Cluster (Azure, AWS and Private Cloud ok).
* No script code changes are required other than entering your Storage Bucket and Credentials at the top of each script.
* Familiarity with Python and PySpark is highly recommended.

## Project Setup

Clone this GitHub repository to your local machine or the VM where you will be running the script.

```
mkdir ~/Documents/cde_ace_hol
cd ~/Documents/cde_ace_hol
git clone https://github.com/pdefusco/CDE_Tour_ACE_HOL.git
```

Alternatively, if you don't have GitHub create a folder on your local computer; navigate to [this URL](https://github.com/pdefusco/CDE_Tour_ACE_HOL.git) and download the files.


#### Part 1: Implement a Spark Pipeline

Using scripts "01_Pre_Setup.py", "02_EnrichData_ETL.py", "03_Spark2Iceberg.py" and "04_Sales_Report.py", you will create four Spark jobs. Along the way you will learn to use CDE Resources, migrate Spark tables to Iceberg tables in the recommended way, and create an incremental report with Iceberg's native Time Travel capability.

#### Part 2: Orchestrating Pipelines with Airflow

You will build three Airflow jobs to schedule, orchestrate and monitor actions of different types.

In the first, you will build a basic Airflow DAG to break up a similar pipeline to the one you built in Part 1. This will be a simple sequence of two Spark jobs.

In the second DAG you will get a tour of Airflow's most recommended operators in CDE. You will familiarize yourself with operators to run bash commands, python methods, and interact with external 3rd party systems.

Airflow task scheduling allows you to implement advanced logic and interdependencies within your pipelines. With the last Airflow DAG, you will get a flavor of Airflow task scheduling with a simple workflow that breaks up a Spark Join into three separate joins.

#### Part 3: Using the CDE CLI

The majority of large scale CDE implementations rely on the CDE API and CLI. With them, you can easily interact with CDE from a local IDE and build integrations with external 3rd party systems. For example, you can implement multi-CDE cluster workflows with GitLabCI or Python.  

In this part of the workshop you will gain familiarity with the CDE CLI by rerunning the same jobs and interacting with the service remotely.
