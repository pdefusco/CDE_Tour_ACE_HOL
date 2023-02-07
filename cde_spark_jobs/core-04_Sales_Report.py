#****************************************************************************
# (C) Cloudera, Inc. 2020-2022
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
# #  Author(s): Paul de Fusco
#***************************************************************************/

# NB: THIS SCRIPT REQUIRES A CDE SPARK 3 CLUSTER

#---------------------------------------------------
#               CREATE SPARK SESSION
#---------------------------------------------------

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
import sys
import utils
import configparser

config = configparser.ConfigParser()
config.read('/app/mount/parameters.conf')
data_lake_name=config.get("general","data_lake_name")
s3BucketName=config.get("general","s3BucketName")
username=config.get("general","username")

print("Running as Username: ", username)

spark = SparkSession \
    .builder \
    .appName("CAR SALES REPORT") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")\
    .config("spark.sql.catalog.spark_catalog.type", "hive")\
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")\
    .config("spark.yarn.access.hadoopFileSystems", data_lake_name)\
    .getOrCreate()

#---------------------------------------------------
#               LOAD ICEBERG TABLES AS DATAFRAMES
#---------------------------------------------------

car_sales_df = spark.sql("SELECT * FROM spark_catalog.{}_CAR_DATA.CAR_SALES".format(username))
customer_data_df = spark.sql("SELECT * FROM spark_catalog.{}_CAR_DATA.CUSTOMER_DATA".format(username))

#---------------------------------------------------
#               LOAD NEW BATCH DATA
#---------------------------------------------------

batch_df = spark.read.csv(s3BucketName + "/10012020_car_sales.csv", header=True, inferSchema=True)
#batch_etl_df.write.mode("overwrite").saveAsTable('{}_CAR_DATA.CAR_SALES'.format(username), format="parquet")

# Creating Temp View for MERGE INTO command
batch_df.createOrReplaceTempView('{}_CAR_SALES_TEMP'.format(username))
print("\n")
print("COMPARING CAR SALES AND CAR SALES TEMP TABLES")
print("SELECT * FROM spark_catalog.{}_CAR_DATA.CAR_SALES".format(username))
spark.sql("SELECT * FROM spark_catalog.{}_CAR_DATA.CAR_SALES".format(username)).show()
print("\n")
print("SELECT * FROM {}_CAR_SALES_TEMP".format(username))
spark.sql("SELECT * FROM {}_CAR_SALES_TEMP".format(username)).show()

#---------------------------------------------------
#               ICEBERG PARTITION EVOLUTION
#---------------------------------------------------

print("CAR SALES TABLE PARTITIONS BEFORE ALTER PARTITION STATEMENT: ")
spark.sql("SELECT * FROM spark_catalog.{}_CAR_DATA.CAR_SALES.PARTITIONS".format(username)).show()

print("REPLACE PARTITION FIELD MONTH WITH FIELD DAY:")
print("ALTER TABLE spark_catalog.{}_CAR_DATA.CAR_SALES REPLACE PARTITION FIELD MONTH WITH DAY")
spark.sql("ALTER TABLE spark_catalog.{}_CAR_DATA.CAR_SALES REPLACE PARTITION FIELD month WITH day".format(username))
#spark.sql("ALTER TABLE prod.db.sample ADD PARTITION FIELD month")

print("CAR SALES TABLE PARTITIONS AFTER ALTER PARTITION STATEMENT: ")
spark.sql("SELECT * FROM spark_catalog.{}_CAR_DATA.CAR_SALES.PARTITIONS".format(username)).show()

#---------------------------------------------------
#               ICEBERG SCHEMA EVOLUTION
#---------------------------------------------------

# DROP COLUMNS
print("EXECUTING ICEBERG DROP COLUMN STATEMENT:")
print("ALTER TABLE spark_catalog.{}_CAR_DATA.CAR_SALES DROP COLUMN VIN".format(username))
spark.sql("ALTER TABLE spark_catalog.{}_CAR_DATA.CAR_SALES DROP COLUMN VIN".format(username))

# CAST COLUMN TO BIGINT
#print("EXECUTING ICEBERG TYPE CONVERSION STATEMENT")
#print("ALTER TABLE {}_CAR_DATA.CAR_SALES ALTER COLUMN CUSTOMER_ID TYPE BIGINT".format(username))
#spark.sql("ALTER TABLE {}_CAR_DATA.CAR_SALES ALTER COLUMN CUSTOMER_ID TYPE BIGINT".format(username))

#---------------------------------------------------
#               ICEBERG MERGE INTO
#---------------------------------------------------

# PRE-INSERT COUNT
print("\n")
print("PRE-MERGE COUNT")
spark.sql("SELECT COUNT(*) FROM spark_catalog.{}_CAR_DATA.CAR_SALES".format(username)).show()

ICEBERG_MERGE_INTO = "MERGE INTO spark_catalog.{0}_CAR_DATA.CAR_SALES t USING (SELECT CUSTOMER_ID, MODEL, SALEPRICE, DAY, MONTH, YEAR FROM {0}_CAR_SALES_TEMP) s ON t.customer_id = s.customer_id WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *".format(username)

#s.model = 'Model Q' THEN UPDATE SET t.saleprice = t.saleprice - 100\
#WHEN MATCHED AND s.model = 'Model R' THEN UPDATE SET t.saleprice = t.saleprice + 10\
print("\n")
print("EXECUTING ICEBERG MERGE INTO QUERY")
print(ICEBERG_MERGE_INTO)
spark.sql(ICEBERG_MERGE_INTO)

# PRE-INSERT COUNT
print("\n")
print("POST-MERGE COUNT")
spark.sql("SELECT COUNT(*) FROM spark_catalog.{}_CAR_DATA.CAR_SALES".format(username)).show()

#---------------------------------------------------
#               ICEBERG TABLE HISTORY AND SNAPSHOTS
#---------------------------------------------------

# ICEBERG TABLE HISTORY (SHOWS EACH SNAPSHOT AND TIMESTAMP)
print("SHOW ICEBERG TABLE HISTORY")
print("SELECT * FROM spark_catalog.{}_CAR_DATA.CAR_SALES.history;".format(username))
spark.sql("SELECT * FROM spark_catalog.{}_CAR_DATA.CAR_SALES.history;".format(username)).show()

# ICEBERG TABLE SNAPSHOTS (USEFUL FOR INCREMENTAL QUERIES AND TIME TRAVEL)
print("SHOW ICEBERG TABLE SNAPSHOTS")
print("SELECT * FROM spark_catalog.{}_CAR_DATA.CAR_SALES.snapshots;".format(username))
spark.sql("SELECT * FROM spark_catalog.{}_CAR_DATA.CAR_SALES.snapshots;".format(username)).show()

#---------------------------------------------------
#               RUNNING DATA QUALITY TESTS
#---------------------------------------------------

# Test 1: Ensure Customer ID is Present so Join Can Happen
print("RUNNING DATA QUALITY TESTS WITH QUINN LIBRARY")
utils.test_column_presence(car_sales_df, ["customer_id"])
utils.test_column_presence(customer_data_df, ["customer_id"])

# Test 2: Spot Nulls or Blanks in Customer Data Sale Price Column:
car_sales_df = utils.test_null_presence_in_col(car_sales_df, "saleprice")

# Test 3:
#customer_data_df = utils.test_values_not_in_col(customer_data_df, ["99999", "11111", "00000"], "zip")

#---------------------------------------------------
#               JOIN CUSTOMER AND SALES DATA
#---------------------------------------------------

#spark.sql("DROP TABLE IF EXISTS spark_catalog.{0}_CAR_DATA.CAR_SALES_REPORTS PURGE".format(username))
print("EXECUTING ICEBERG CREATE OR REPLACE TABLE STATEMENT")
print("CREATE OR REPLACE TABLE spark_catalog.{0}_CAR_DATA.SALES_REPORT USING ICEBERG AS SELECT s.MODEL, s.SALEPRICE, c.SALARY, c.GENDER, c.EMAIL FROM spark_catalog.{0}_CAR_DATA.CAR_SALES s INNER JOIN spark_catalog.{0}_CAR_DATA.CUSTOMER_DATA c on s.CUSTOMER_ID = c.CUSTOMER_ID".format(username))
spark.sql("CREATE OR REPLACE TABLE spark_catalog.{0}_CAR_DATA.SALES_REPORT USING ICEBERG AS SELECT s.MODEL, s.SALEPRICE, c.SALARY, c.GENDER, c.EMAIL FROM spark_catalog.{0}_CAR_DATA.CAR_SALES s INNER JOIN spark_catalog.{0}_CAR_DATA.CUSTOMER_DATA c on s.CUSTOMER_ID = c.CUSTOMER_ID".format(username))

#---------------------------------------------------
#               ANALYTICAL QUERIES
#---------------------------------------------------

reports_df = spark.sql("SELECT * FROM {}_CAR_DATA.SALES_REPORT".format(username))

print("GROUP TOTAL SALES BY MODEL")
model_sales_df = reports_df.groupBy("Model").sum("Saleprice").na.drop().sort(F.asc('sum(Saleprice)')).withColumnRenamed("sum(Saleprice)", "sales_by_model")
model_sales_df = model_sales_df.withColumn('total_sales_by_model', model_sales_df.sales_by_model.cast(DecimalType(18, 2)))
model_sales_df.select(["Model", "total_sales_by_model"]).sort(F.asc('Model')).show()

print("GROUP TOTAL SALES BY GENDER")
gender_sales_df = reports_df.groupBy("Gender").sum("Saleprice").na.drop().sort(F.asc('sum(Saleprice)')).withColumnRenamed("sum(Saleprice)", "sales_by_gender")
gender_sales_df = gender_sales_df.withColumn('total_sales_by_gender', gender_sales_df.sales_by_gender.cast(DecimalType(18, 2)))
gender_sales_df.select(["Gender", "total_sales_by_gender"]).sort(F.asc('Gender')).show()
