from pyspark.sql import SparkSession
#from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import sys
#from pyspark.sql.functions import year, month, dayofmonth, dayofweek, dayofyear, weekofyear

#---------------------------------------------------
#               ENTER YOUR USERNAME HERE
#---------------------------------------------------
username = "test_user_112222_3"
data_lake_name = "s3a://go01-demo/"

print("Running script with Username: ", username)

#---------------------------------------------------
#               CREATE SPARK SESSION
#---------------------------------------------------
spark = SparkSession\
            .builder\
            .appName('CAR INSTALLS ETL')\
            .config("spark.yarn.access.hadoopFileSystems", data_lake_name)\
            .getOrCreate()


car_installs_df  = spark.sql("SELECT * FROM {}_CAR_DATA.car_installs".format(username))
factory_data_df  = spark.sql("SELECT * FROM {}_CAR_DATA.experimental_motors".format(username))

installs_report_df = car_installs_df.join(factory_data_df, "serial_no")

print("Car Installs and Factory Data Dataframe")
installs_report_df.show()

print("ETL Step 1: Create TimeStamp Column")
installs_etl_step1_df = installs_report_df.withColumn("time", F.col("timestamp").cast("timestamp"))
installs_etl_step1_df.show()

print("ETL Step 2: Extract Date Columns")
installs_etl_step2_df = installs_etl_step1_df\
                        .withColumn("dayofmonth", F.dayofmonth("time"))\
                        .withColumn("month", F.month("time"))\
                        .withColumn("year", F.year("time"))\
                        .withColumn("dayofweek", F.dayofweek("time"))\
                        .withColumn("dayofyear", F.dayofyear("time"))\
                        .withColumn("weekofyear", F.weekofyear("time"))

installs_etl_step2_df.show()

installs_etl_step2_df.write.mode("overwrite").saveAsTable('{}_CAR_DATA.INSTALLS_ETL'.format(username), format="parquet")
