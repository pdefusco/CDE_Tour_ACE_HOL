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

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import sys
import configparser

config = configparser.ConfigParser()
config.read('/app/mount/parameters.conf')
data_lake_name=config.get("general","data_lake_name")
s3BucketName=config.get("general","s3BucketName")
username=config.get("general","username")

print("Running as Username: ", username)

#---------------------------------------------------
#               CREATE SPARK SESSION
#---------------------------------------------------
spark = SparkSession\
            .builder\
            .appName('CAR INSTALLS REPORT')\
            .config("spark.yarn.access.hadoopFileSystems", data_lake_name)\
            .getOrCreate()

installs_etl_step2_df = spark.sql("SELECT * FROM {}_CAR_DATA.INSTALLS_ETL".format(username))

print("Report: Weekly Count of Parts Installed")
installs_etl_step3_df = installs_etl_step2_df.drop("timestamp").groupBy("weekofyear").count().orderBy("weekofyear", asc=True)
installs_etl_step3_df.show()

print("Report: Weekly Cumulative of Parts Installed")
installs_etl_step4_df = installs_etl_step3_df.withColumn('count_percent',F.col('count')/F.sum('count').over(Window.partitionBy())*100)
installs_etl_step5_df = installs_etl_step4_df.withColumn('cum_percent', F.sum(installs_etl_step4_df.count_percent).over(Window.partitionBy().orderBy().rowsBetween(-sys.maxsize, 0)))
installs_etl_step5_df.show()
