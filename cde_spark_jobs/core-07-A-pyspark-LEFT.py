from pyspark.sql import SparkSession
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType
import os
import sys
from time import sleep

data_lake_name = "s3a://go01-demo/"
s3BucketName = "s3a://go01-demo/cde-workshop/cardata-csv/"
# Your Username Here:
username = "test_user_112222_7"

print("Running script with Username: ", username)

spark = SparkSession \
    .builder \
    .appName("PySpark SQL") \
    .config("spark.yarn.access.hadoopFileSystems", data_lake_name)\
    .getOrCreate()

# A list of tuples
tuples = [("John", 19), ("Smith", 23), ("Sarah", 18), ("Mark", 39), ("Silvio", 45)]

# Schema with two fields - person_name and person_age
schema = StructType([StructField("person_name", StringType(), False),
                    StructField("person_age", IntegerType(), False)])

# Create a DataFrame by applying the schema to the RDD and print the schema
left_df = spark.createDataFrame(tuples, schema)
left_df.printSchema()

sleep(20)

for each in left_df.collect():
    print(each[0])

try:
    left_df.write.mode("overwrite").saveAsTable('{}_CAR_DATA.LEFT_TABLE'.format(username), format="parquet")
except:
    pass

spark.stop()
