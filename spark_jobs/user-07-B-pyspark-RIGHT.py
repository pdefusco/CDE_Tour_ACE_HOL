from pyspark.sql import SparkSession
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType
import os
import sys

data_lake_name = "s3a://go01-demo/"
s3BucketName = "s3a://go01-demo/cde-workshop/cardata-csv/"
# Your Username Here:
username = "test_user_111722_1"

print("Running script with Username: ", username)

spark = SparkSession \
    .builder \
    .appName("PySpark SQL") \
    .config("spark.yarn.access.hadoopFileSystems", data_lake_name)\
    .getOrCreate()

# A list of Rows. Infer schema from the first row, create a DataFrame and print the schema
rows = [Row(name="John", age=19), Row(name="Smith", age=23), Row(name="Sarah", age=18), Row(name="Oscar", age=50), Row(name="Marta", age=29)]
right_df = spark.createDataFrame(rows)
right_df.printSchema()

try:
    right_df.write.mode("overwrite").saveAsTable('{}_CAR_DATA.RIGHT_TABLE'.format(username), format="parquet")
except:
    pass

spark.stop()
