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

# NB: THIS SCRIPT REQUIRES A SPARK 3 CLUSTER

#---------------------------------------------------
#               CREATE SPARK SESSION
#---------------------------------------------------

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
import sys
import utils

data_lake_name = "s3a://go01-demo/"
s3BucketName = "s3a://go01-demo/cde-workshop/cardata-csv/"
# Your Username Here:
username = "test_user_111622_2"

print("Running script with Username: ", username)

spark = SparkSession \
    .builder \
    .appName("Car Sales Report") \
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

batch_df = spark.read.csv(s3BucketName + "10012020_car_sales.csv", header=True, inferSchema=True)
#batch_etl_df.write.mode("overwrite").saveAsTable('{}_CAR_DATA.CAR_SALES'.format(username), format="parquet")

# Creating Temp View for MERGE INTO command
batch_df.createOrReplaceTempView('{}_CAR_SALES_TEMP'.format(username))

#spark.sql("SELECT * FROM {}_CAR_DATA.CAR_SALES_TEMP")

#---------------------------------------------------
#               ICEBERG MERGE INTO
#---------------------------------------------------

print(car_sales_df.dtypes)
print('\n')
print(batch_df.dtypes)

ICEBERG_MERGE_INTO = "MERGE INTO spark_catalog.{0}_CAR_DATA.CAR_SALES t\
                            USING (SELECT * FROM {0}_CAR_SALES_TEMP) s\
                            ON t.CUSTOMER_ID = s.CUSTOMER_ID\
                            WHEN MATCHED AND s.MODEL = 'Model C' AND s.SALEPRICE > 100000 THEN DELETE\
                            WHEN NOT MATCHED THEN INSERT *".format(username)

customer_data_df = spark.sql(ICEBERG_MERGE_INTO)

print('\n')
print('Customer Data DF Columns')
print(customer_data_df.dtypes)

#---------------------------------------------------
#               ICEBERG TABLE HISTORY AND SNAPSHOTS
#---------------------------------------------------

# ICEBERG TABLE HISTORY (SHOWS EACH SNAPSHOT AND TIMESTAMP)
spark.sql("SELECT * FROM {}_CAR_DATA.CAR_SALES.history;".format(username)).show()

# ICEBERG TABLE SNAPSHOTS (USEFUL FOR INCREMENTAL QUERIES AND TIME TRAVEL)
spark.sql("SELECT * FROM {}_CAR_DATA.CAR_SALES.snapshots;".format(username)).show()

#---------------------------------------------------
#               RUNNING DATA QUALITY TESTS
#---------------------------------------------------

# Test 1: Ensure Customer ID is Present so Join Can Happen
utils.test_column_presence(car_sales_df, ["customer_id"])
utils.test_column_presence(customer_data_df, ["customer_id"])

# Test 2: Spot Nulls or Blanks in Customer Data Sale Price Column:
car_sales_df = utils.test_null_presence_in_col(car_sales_df, "saleprice")

# Test 3:
#customer_data_df = utils.test_values_not_in_col(customer_data_df, ["99999", "11111", "00000"], "zip")

#---------------------------------------------------
#               JOIN CUSTOMER AND SALES DATA
#---------------------------------------------------

report_df = car_sales_df.join(customer_data_df, "customer_id")
report_df.write.mode("overwrite").saveAsTable('{}_CAR_DATA.REPORT_FACT_TABLE'.format(username), format="parquet")
#report_df.createOrReplaceTempView('{}_REPORT_FACT_VIEW'.format(username))

spark.sql("ALTER TABLE {}_CAR_DATA.REPORT_FACT_TABLE UNSET TBLPROPERTIES ('TRANSLATED_TO_EXTERNAL')".format(username))
spark.sql("CALL spark_catalog.system.migrate('{}_CAR_DATA.REPORT_FACT_TABLE')".format(username))

#---------------------------------------------------
#               ICEBERG SCHEMA EVOLUTION
#---------------------------------------------------

# DROP COLUMNS
spark.sql("ALTER TABLE {}_CAR_DATA.REPORT_FACT_TABLE DROP COLUMN CUSTOMER_ID".format(username))
spark.sql("ALTER TABLE {}_CAR_DATA.REPORT_FACT_TABLE DROP COLUMN VIN".format(username))
spark.sql("ALTER TABLE {}_CAR_DATA.REPORT_FACT_TABLE DROP COLUMN USERNAME".format(username))
spark.sql("ALTER TABLE {}_CAR_DATA.REPORT_FACT_TABLE DROP COLUMN NAME".format(username))
spark.sql("ALTER TABLE {}_CAR_DATA.REPORT_FACT_TABLE DROP COLUMN EMAIL".format(username))
spark.sql("ALTER TABLE {}_CAR_DATA.REPORT_FACT_TABLE DROP COLUMN OCCUPATION".format(username))
spark.sql("ALTER TABLE {}_CAR_DATA.REPORT_FACT_TABLE DROP COLUMN BIRTHDATE".format(username))
spark.sql("ALTER TABLE {}_CAR_DATA.REPORT_FACT_TABLE DROP COLUMN ADDRESS".format(username))
spark.sql("ALTER TABLE {}_CAR_DATA.REPORT_FACT_TABLE DROP COLUMN SALARY".format(username))
spark.sql("ALTER TABLE {}_CAR_DATA.REPORT_FACT_TABLE DROP COLUMN ZIP".format(username))

# CAST COLUMN TO FLOAT
spark.sql("ALTER TABLE {}_CAR_DATA.REPORT_FACT_TABLE COLUMN SALEPRICE FLOAT".format(username))

#---------------------------------------------------
#               ANALYTICAL QUERIES
#---------------------------------------------------

fact_df = spark.sql("SELECT * FROM {}_CAR_DATA.REPORT_FACT_TABLE".format(username))

#GROUP TOTAL SALES BY MONTH
month_sales_df = fact_df.groupBy("Month").sum("Price").na.drop().sort(F.asc('sum(Price)')).withColumnRenamed("sum(Price)", "sales_by_month")
month_sales_df = month_sales_df.withColumn('total_sales_by_month', month_sales_df.sales_by_month.cast(DecimalType(18, 2)))
month_sales_df.select(["Month", "total_sales_by_month"]).sort(F.asc('Month')).show()

#GROUP TOTAL SALES BY MODEL
model_sales_df = fact_df.groupBy("model").sum("Price").na.drop().sort(F.asc('sum(Price)')).withColumnRenamed("sum(Price)", "sales_by_model")
model_sales_df = model_sales_df.withColumn('total_sales_by_model', model_sales_df.sales_by_model.cast(DecimalType(18, 2)))
model_sales_df.select(["model", "total_sales_by_model"]).sort(F.asc('model')).show()

#GROUP TOTAL SALES BY GENDER
gender_sales_df = fact_df.groupBy("gender").sum("Price").na.drop().sort(F.asc('sum(Price)')).withColumnRenamed("sum(Price)", "sales_by_gender")
gender_sales_df = gender_sales_df.withColumn('total_sales_by_gender', gender_sales_df.sales_by_gender.cast(DecimalType(18, 2)))
gender_sales_df.select(["gender", "total_sales_by_gender"]).sort(F.asc('gender')).show()
