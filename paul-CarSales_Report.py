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
from quinn.extensions import *
import quinn
import pyspark.sql.functions as F
import sys

data_lake_name = "s3a://go01-demo/"
s3BucketName = "s3a://go01-demo/cde-workshop/parquet_data"

# Your Username Here:
username = "paul"

spark = SparkSession \
    .builder \
    .appName("Car Sales Report") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")\
    .config("spark.sql.catalog.spark_catalog.type", "hive")\
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")\
    .config("spark.sql.adaptive.enabled", false)\
    .config("spark.yarn.access.hadoopFileSystems", data_lake_name)\
    .getOrCreate()

#---------------------------------------------------
#               LOAD ICEBERG TABLES AS DATAFRAMES
#---------------------------------------------------

car_sales_df = spark.sql("SELECT * FROM spark_catalog.{}_CAR_DATA.CAR_SALES".format(username))
customer_data_df = spark.sql("SELECT * FROM spark_catalog.{}_CAR_DATA.CUSTOMER_DATA".format(username))

#---------------------------------------------------
#               DATA QUALITY TESTS WITH QUINN LIB
#---------------------------------------------------

def test_column_presence(spark_df, col_list):
    print("Testing for existence of: {}", [i for i in col_list])

    try:
        print(quinn.validate_presence_of_columns(spark_df, col_list))
        print("All Test Columns Found")
    except:
        print("One or More Columns From List Not Found")


def test_null_presence_in_col(spark_df, col):
    print("Testing for blank or null in Column: ", col)

    try:
        df = spark_df.withColumn("is_blank_or_null", F.col(col).isNullOrBlank())
        print("Dataframe enriched with test")
        return df
    except:
        print("Error During Test")

def test_values_not_in_col(spark_df, value_list, col):
    print("Testing for exclusion of values: ")
    print(" ".join(value_list))
    print("from Column: {}".format(col))

    try:
        df = spark_df.withColumn("{}_is_not_in_val_list".format(col), F.col(col).isNotIn(value_list))
        print("Dataframe enriched with test")
        return df
    except:
        print("Error During Test")

#---------------------------------------------------
#               RUNNING DATA QUALITY TESTS
#---------------------------------------------------

# Test 1: Ensure Customer ID is Present so Join Can Happen
test_column_presence(car_sales_df, ["customer_id"])
test_column_presence(customer_data_df, ["customer_id"])

# Test 2: Spot Nulls or Blanks in Customer Data Sale Price Column:
car_sales_df = test_null_presence_in_col(car_sales_df, "salesprice")

# Test 3:
customer_data_df = test_values_not_in_col(customer_data_df, ["23356", "99803", "31750"], zip)

#---------------------------------------------------
#               JOIN CUSTOMER AND SALES DATA
#---------------------------------------------------

report_df = car_sales_df.join(car_sales_df, "customer_id")

#---------------------------------------------------
#               A FEW MORE ETL STEPS
#---------------------------------------------------

key_cols = ["model", "saleprice", "sale_date", "gender"]
key_attributes_df = report_df.select(*key_cols)
key_attributes_df.show()

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

df1 = key_attributes_df.withColumn("Date",to_date(col("sale_date"),"MM/dd/yyyy"))
df1.show()

df2 = df1.withColumn("Month", month("Date"))
df2.show()

df3 = df2.withColumn("Price", col("saleprice").cast("float"))
df3.show()

df4 = df3.select(["model", "gender", "Month", "Price"])
df4.show()

df5 = df4.withColumn("model", regexp_replace("model", "Model", ""))
df5.show()

#---------------------------------------------------
#               ANALYTICAL QUERIES
#---------------------------------------------------

#Group By Total Sales by Month
month_sales_df = df5.groupBy("Month").sum("Price").na.drop().sort(asc('sum(Price)')).withColumnRenamed("sum(Price)", "sales_by_month")
month_sales_df = month_sales_df.withColumn('total_sales_by_month', month_sales_df.sales_by_month.cast(DecimalType(18, 2)))
month_sales_df.select(["Month", "total_sales_by_month"]).sort(asc('Month')).show()

#Group By Total Sales by Month
model_sales_df = df5.groupBy("model").sum("Price").na.drop().sort(asc('sum(Price)')).withColumnRenamed("sum(Price)", "sales_by_model")
model_sales_df = model_sales_df.withColumn('total_sales_by_model', model_sales_df.sales_by_model.cast(DecimalType(18, 2)))
model_sales_df.select(["model", "total_sales_by_model"]).sort(asc('model')).show()

#Group By Total Sales by Gender
gender_sales_df = df5.groupBy("gender").sum("Price").na.drop().sort(asc('sum(Price)')).withColumnRenamed("sum(Price)", "sales_by_gender")
gender_sales_df = gender_sales_df.withColumn('total_sales_by_gender', gender_sales_df.sales_by_gender.cast(DecimalType(18, 2)))
gender_sales_df.select(["gender", "total_sales_by_gender"]).sort(asc('gender')).show()
