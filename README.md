# CDE Tour ACE Workshop HOL

## Objective

CDE is the Cloudera Data Engineering Service, a containerized managed service for Large Scale Batch Pipelines with Spark featuring Airflow and Iceberg.

This CDE Hands On Lab is designed to walk you through the Services's main capabilities. Throughout the exercises you will:

1. Deploy an Ingestion, Transformation and Reporting pipeline with Spark 3.2.
2. Learn about Iceberg's most popular features.
3. Orchestrate pipelines with Airflow.

#### Part 1: Implement a Spark Pipeline

Using scripts "01_Pre_Setup.py", "02_EnrichData_ETL.py", "03_Spark2Iceberg.py" and "04_Sales_Report.py", you will create four Spark jobs. Along the way you will learn to use CDE Resources, migrate Spark tables to Iceberg tables in the recommended way, and create an incremental report with Iceberg's native Time Travel capability.

#### Part 2: Orchestrating Pipelines with Airflow

You will build three Airflow jobs to schedule, orchestrate and monitor actions of different types.

In the first, you will build a basic Airflow DAG to break up a similar pipeline to the one you built in Part 1. This will be a simple sequence of two Spark jobs.

In the second DAG you will get a tour of Airflow's most recommended operators in CDE. You will familiarize yourself with operators to run bash commands, python methods, and interact with external 3rd party systems.

Airflow task scheduling allows you to implement advanced logic and interdependencies within your pipelines. With the last Airflow DAG, you will get a flavor of Airflow task scheduling with a simple workflow that breaks up a Spark Join into three separate joins.

#### Part 3: Using the CDE API

The majority of large scale CDE implementations rely on the CDE API and CLI. With them, you can easily interact with CDE from a local IDE and build integrations with external 3rd party systems. For example, you can implement multi-CDE cluster workflows with GitLabCI or Python.  

In this part of the workshop you will gain familiarity with the CDE API by rerunning the same jobs and interacting with the service remotely.

## Step by Step Instructions

Detailed instructions in English are provided in the [step_by_step_guides](https://github.com/pdefusco/CDE_First_Step/tree/main/step_by_step_guides) folder.


## Conclusions & Next Steps

CDE is the Cloudera Data Engineering Service, a containerized managed service for Spark and Airflow.

In this workshop we explored some of CDE's most important features. Most importantly, CDE is the

If you are exploring CDE you may find the following tutorials relevant:

* [Spark 3 & Iceberg](https://github.com/pdefusco/Spark3_Iceberg_CML): A quick intro of Time Travel Capabilities with Spark 3.

* [Simple Intro to the CDE CLI](https://github.com/pdefusco/CDE_CLI_Simple): An introduction to the CDE CLI for the CDE beginner.

* [CDE CLI Demo](https://github.com/pdefusco/CDE_CLI_demo): A more advanced CDE CLI reference with additional details for the CDE user who wants to move beyond the basics.

* [CDE Resource 2 ADLS](https://github.com/pdefusco/CDEResource2ADLS): An example integration between ADLS and CDE Resource. This pattern is applicable to AWS S3 as well and can be used to pass execution scripts, dependencies, and virtually any file from CDE to 3rd party systems and viceversa.

* [Using CDE Airflow](https://github.com/pdefusco/Using_CDE_Airflow): A guide to Airflow in CDE including examples to integrate with 3rd party systems via Airflow Operators such as BashOperator, HttpOperator, PythonOperator, and more.

* [GitLab2CDE](https://github.com/pdefusco/Gitlab2CDE): a CI/CD pipeline to orchestrate Cross-Cluster Workflows for Hybrid/Multicloud Data Engineering.

* [CML2CDE](https://github.com/pdefusco/cml2cde_api_example): an API to create and orchestrate CDE Jobs from any Python based environment including CML. Relevant for ML Ops or any Python Users who want to leverage the power of Spark in CDE via Python requests.

* [Postman2CDE](https://github.com/pdefusco/Postman2CDE): An example of the Postman API to bootstrap CDE Services with the CDE API.

* [Oozie2CDEAirflow API](https://github.com/pdefusco/Oozie2CDE_Migration): An API to programmatically convert Oozie workflows and dependencies into CDE Airflow and CDE Jobs. This API is designed to easily migrate from Oozie to CDE Airflow and not just Open Source Airflow.

For more information on the Cloudera Data Platform and its form factors please visit [this site](https://docs.cloudera.com/).

For more information on migrating Spark jobs to CDE, please reference [this guide](https://docs.cloudera.com/cdp-private-cloud-upgrade/latest/cdppvc-data-migration-spark/topics/cdp-migration-spark-cdp-cde.html).

If you have any questions about CML or would like to see a demo, please reach out to your Cloudera Account Team or send a message [through this portal](https://www.cloudera.com/contact-sales.html) and we will be in contact with you soon.
