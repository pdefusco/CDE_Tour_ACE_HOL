#---------------------------------------------------
#               DATA QUALITY TESTS WITH QUINN LIB
#---------------------------------------------------

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from quinn.extensions import *
import quinn
import sys


def test_column_presence(spark_df, col_list):
    print("Testing for existence of Columns: ", [i for i in col_list])

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
    except:
        print("Error During Test")

    return df
'''
def test_values_not_in_col(spark_df, value_list, col):
    print("Testing for exclusion of values: ")
    print(" ".join(value_list))
    print("from Column: {}".format(col))

    try:
        df = spark_df.withColumn("is_not_in_val_list", F.col(col).isNotIn(value_list))
        print("Dataframe enriched with test")
    except:
        print("Error During Test")

    return df
'''
