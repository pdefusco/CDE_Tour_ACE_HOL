#****************************************************************************
# (C) Cloudera, Inc. 2020-2021
#  All rights reserved.
#
#  Applicable Open Source License: GNU Affero General Public License v3.0
#
#  NOTE: Cloudera open source products are modular software products
#  made up of hundreds of individual components, each of which was
#  individually copyrighted.  Each Cloudera open source product is a
#  collective work under U.S. Copyright Law. Your license to use the
#  collective work is as provided in your written agreement with
#  Cloudera.  Used apart from the collective work, this file is
#  licensed for your use pursuant to the open source license
#  identified above.
#
#  This code is provided to you pursuant a written agreement with
#  (i) Cloudera, Inc. or (ii) a third-party authorized to distribute
#  this code. If you do not have a written agreement with Cloudera nor
#  with an authorized and properly licensed third party, you do not
#  have any rights to access nor to use this code.
#
#  Absent a written agreement with Cloudera, Inc. (“Cloudera”) to the
#  contrary, A) CLOUDERA PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY
#  KIND; (B) CLOUDERA DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED
#  WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT LIMITED TO
#  IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE; (C) CLOUDERA IS NOT LIABLE TO YOU,
#  AND WILL NOT DEFEND, INDEMNIFY, NOR HOLD YOU HARMLESS FOR ANY CLAIMS
#  ARISING FROM OR RELATED TO THE CODE; AND (D)WITH RESPECT TO YOUR EXERCISE
#  OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, CLOUDERA IS NOT LIABLE FOR ANY
#  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR
#  CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO, DAMAGES
#  RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF
#  BUSINESS ADVANTAGE OR UNAVAILABILITY, OR LOSS OR CORRUPTION OF
#  DATA.
#
#  Source File Name: Pre-SetupDW.py
#
#  REQUIREMENT: Update variable s3BucketName
#               using storage.location.base attribute; defined by your environment.
#
#
# #  Author(s): Paul de Fusco
#***************************************************************************/
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F

#---------------------------------------------------
#               ENTER YOUR USERNAME HERE
#---------------------------------------------------

data_lake_name = "s3a://go01-demo/" # <--- Update data lake val
s3BucketName = "s3a://go01-demo/cde-workshop/cardata-csv" # <--- Update bucket location
# Your Username Here:
username = "test_user_112122_1"

print("Running script with Username: ", username)

#---------------------------------------------------
#               CREATE SPARK SESSION
#---------------------------------------------------
spark = SparkSession.builder.appName('Ingest').config("spark.yarn.access.hadoopFileSystems", data_lake_name).getOrCreate()

#-----------------------------------------------------------------------------------
# LOAD DATA FROM .CSV FILES ON AWS S3 CLOUD STORAGE
#
# REQUIREMENT: Update variable s3BucketName
#              using storage.location.base attribute; defined by your environment.
#
#              For example, property storage.location.base
#                           has value 's3a://usermarketing-cdp-demo'
#                           Therefore, set variable as:
#                                 s3BucketName = "s3a://usermarketing-cdp-demo"
#-----------------------------------------------------------------------------------
car_installs  = spark.read.csv(s3BucketName + "/car_installs.csv",        header=True, inferSchema=True)
car_sales     = spark.read.csv(s3BucketName + "/historical_car_sales.csv",           header=True, inferSchema=True)
customer_data = spark.read.csv(s3BucketName + "/customer_data.csv",       header=True, inferSchema=True)
factory_data  = spark.read.csv(s3BucketName + "/experimental_motors.csv", header=True, inferSchema=True)
geo_data      = spark.read.csv(s3BucketName + "/postal_codes.csv",        header=True, inferSchema=True)

#---------------------------------------------------
#       SQL CLEANUP: DATABASES, TABLES, VIEWS
#---------------------------------------------------
print("JOB STARTED...")
spark.sql("DROP DATABASE IF EXISTS {}_CAR_DATA CASCADE".format(username))
print("\tDROP DATABASE(S) COMPLETED")

##---------------------------------------------------
##                 CREATE DATABASES
##---------------------------------------------------
spark.sql("CREATE DATABASE {}_CAR_DATA".format(username))
print("\tCREATE DATABASE(S) COMPLETED")

##---------------------------------------------------
##                 PARTITION CAR SALES DATA
##---------------------------------------------------

print("\n")
print("Current Number of Partitions")
print(car_sales.rdd.getNumPartitions())
print("\n")
print("Repartitioning by Month")
print('car_sales.repartition(12, "month")')
car_sales = car_sales.repartition(12, "month")
print("\n")
print("New Number of Partitions")
print(car_sales.rdd.getNumPartitions())

#---------------------------------------------------
#               POPULATE TABLES
#---------------------------------------------------
car_sales.write.mode("overwrite").saveAsTable('{}_CAR_DATA.CAR_SALES'.format(username), format="parquet")
car_installs.write.mode("overwrite").saveAsTable('{}_CAR_DATA.CAR_INSTALLS'.format(username), format="parquet")
factory_data.write.mode("overwrite").saveAsTable('{}_CAR_DATA.EXPERIMENTAL_MOTORS'.format(username), format="parquet")
customer_data.write.mode("overwrite").saveAsTable('{}_CAR_DATA.CUSTOMER_DATA'.format(username), format="parquet")
geo_data.write.mode("overwrite").saveAsTable('{}_CAR_DATA.GEO_DATA_XREF'.format(username), format="parquet")
print("\tPOPULATE TABLE(S) COMPLETED")

print("JOB COMPLETED.\n\n")
