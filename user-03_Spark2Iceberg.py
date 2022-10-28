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
from datetime import datetime
import sys

data_lake_name = "s3a://go01-demo/"
s3BucketName = "s3a://go01-demo/cde-workshop/car-data/"

# Your Username Here:
username = "user_test_1"

spark = SparkSession \
    .builder \
    .appName("Iceberg Load") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")\
    .config("spark.sql.catalog.spark_catalog.type", "hive")\
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")\
    .config("spark.yarn.access.hadoopFileSystems", data_lake_name)\
    .getOrCreate()

#---------------------------------------------------
#               MIGRATE HIVE TABLES TO ICEBERG TABLE
#---------------------------------------------------
#query_1="""CALL spark_catalog.system.migrate('{}_CAR_DATA.CAR_SALES')""".format(username)
#print(query_1)
#spark.sql(query_1)

#query_2 = """CALL spark_catalog.system.snapshot('{0}_CAR_DATA.CUSTOMER_DATA', '{0}_CAR_DATA.CUSTOMER_DATA_ICE')""".format(username)
#print(query_2)
#spark.sql(query_2)

spark.sql("SELECT * FROM {}_CAR_DATA.car_sales".format(username))

#----------------------------------------------------
#               MIGRATE SPARK TABLES TO ICEBERG TABLE
#----------------------------------------------------
try:
    spark.sql("ALTER TABLE {}_CAR_DATA.CAR_SALES UNSET TBLPROPERTIES ('TRANSLATED_TO_EXTERNAL')".format(username))
    spark.sql("CALL spark_catalog.system.migrate('{}_CAR_DATA.CAR_SALES')".format(username))
    print("Migrated the Car Sales Table to Iceberg Format.")
except:
    print("The Car Sales table has already been migrated to Iceberg Format.")

try:
    spark.sql("ALTER TABLE {}_CAR_DATA.CUSTOMER_DATA UNSET TBLPROPERTIES ('TRANSLATED_TO_EXTERNAL')".format(username))
    spark.sql("CALL spark_catalog.system.migrate('{}_CAR_DATA.CUSTOMER_DATA')".format(username))
    print("Migrated the Customer Data table to Iceberg Format.")
except:
    print("The Customer Data table has already been migrated to Iceberg.")

# Iceberg comes with catalogs that enable SQL commands to manage tables and load them by name.
# Catalogs are configured using properties under spark.sql.catalog.(catalog_name).

# Approach 1: Read Spark Table into Spark Dataframe; Then Use Iceberg Dataframe API to create an Iceberg Table.
#spark.sql("CREATE DATABASE IF NOT EXISTS spark_catalog.iceb")
#spark.sql("USE spark_catalog.iceb")
#spark.sql("SHOW CURRENT NAMESPACE").show()

#query_1="""SELECT * FROM {}_CAR_DATA.CAR_SALES""".format(username)
#query1_df = spark.sql(query_1)
#query1_df.writeTo("spark_catalog.iceb.CAR_SALES").create()

# Alternatively use:
#query1_df.write.format("iceberg").mode("overwrite").save("spark_catalog.iceb.CAR_SALES")

# Approach 2: Create a Spark Temp Table from the Spark Table; Then use Iceberg "CREATE "
#query_2 = """SELECT * FROM {}_CAR_DATA.CAR_SALES""".format(username)
#query2_df = spark.sql(query_2)
#query2_df.createOrReplaceTempView("tempview");
#spark.sql("CREATE or REPLACE TABLE spark_catalog.iceb.CAR_SALES USING iceberg AS SELECT * FROM tempview");

# Approach 3: Create an empty Iceberg Table via Spark SQL and use an INSERT statement to load from an existing Spark Table
#query_3 = """CREATE TABLE IF NOT EXISTS spark_catalog.iceb.car_installs_iceberg (model STRING, VIN STRING, serial_no STRING) USING iceberg"""
#spark.sql(query_3)
#query_4 = """INSERT INTO spark_catalog.iceb.car_installs_iceberg SELECT * FROM {}_CAR_DATA.CAR_INSTALLS""".format(username)

#---------------------------------------------------
#               SHOW ICEBERG TABLE SNAPSHOTS
#---------------------------------------------------

spark.read.format("iceberg").load("spark_catalog.{}_CAR_DATA.CAR_SALES.history".format(username)).show(20, False)

# SAVE TIMESTAMP BEFORE INSERTS
now = datetime.now()

timestamp = datetime.timestamp(now)
print("PRE-INSERT TIMESTAMP: ", timestamp)

#---------------------------------------------------
#               INSERT DATA
#---------------------------------------------------

# PRE-INSERT COUNT
print("PRE-INSERT COUNT")
spark.sql("SELECT COUNT(*) FROM spark_catalog.{}_CAR_DATA.CAR_SALES".format(username)).show()

# INSERT DATA APPROACH 1 - APPEND FROM DATAFRAME
temp_df = spark.sql("SELECT * FROM spark_catalog.{}_CAR_DATA.CAR_SALES".format(username)).sample(fraction=0.3, seed=3)
temp_df.writeTo("spark_catalog.{}_CAR_DATA.CAR_SALES".format(username)).append()

# INSERT DATA APPROACH 2 - INSERT VIA SQL
spark.sql("DROP TABLE IF EXISTS spark_catalog.{}_CAR_DATA.CAR_SALES_SAMPLE".format(username))
temp_df.writeTo("spark_catalog.{}_CAR_DATA.CAR_SALES_SAMPLE".format(username)).create()

print("INSERT DATA VIA SPARK SQL")
query_5 = """INSERT INTO spark_catalog.{0}_CAR_DATA.CAR_SALES SELECT * FROM spark_catalog.{0}_CAR_DATA.CAR_SALES_SAMPLE""".format(username)
print(query_5)
spark.sql(query_5)

#---------------------------------------------------
#               TIME TRAVEL
#---------------------------------------------------

# NOTICE SNAPSHOTS HAVE BEEN ADDED
spark.read.format("iceberg").load("spark_catalog.{}_CAR_DATA.CAR_SALES.history".format(username)).show(20, False)

# POST-INSERT COUNT
print("POST-INSERT COUNT")
spark.sql("SELECT COUNT(*) FROM spark_catalog.{}_CAR_DATA.CAR_SALES".format(username)).show()

# TIME TRAVEL AS OF PREVIOUS TIMESTAMP
df = spark.read.option("as-of-timestamp", int(timestamp*1000)).format("iceberg").load("spark_catalog.{}_CAR_DATA.CAR_SALES".format(username))

# POST TIME TRAVEL COUNT
print("POST-TIME TRAVEL COUNT")
print(df.count())

#---------------------------------------------------
#               SAVE DATA TO PARQUET
#---------------------------------------------------
from datetime import datetime
write_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
temp_df.write.mode("overwrite").option("header", "true").parquet(s3BucketName+write_time+"/car_sales_data.parquet")
