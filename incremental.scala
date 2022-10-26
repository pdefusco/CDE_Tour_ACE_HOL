// SQLContext entry point for working with structured data
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
// this is used to implicitly convert an RDD to a DataFrame.
import sqlContext.implicits._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType, DateType};


import java.sql.Date
import org.apache.spark.sql.{Row, DataFrame}

// Import Spark SQL data types, row and math functions
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


def createDF(rows: Seq[Row], schema: StructType): DataFrame = {
  spark.createDataFrame(
    sc.parallelize(rows),
    schema
  )
}

val schema = StructType(
  List(
    StructField("order_no", StringType, true),
    StructField("customer_id", StringType, true),
    StructField("quantity", IntegerType, true),
    StructField("cost", DoubleType, true),
    StructField("order_date", DateType, true),
    StructField("last_updated_date", DateType, true)
  )
)

// Create orders dataframe
val orders = Seq(
  Row(

    "001", "u1", 1, 15.00,

    Date.valueOf("2020-03-01"), Date.valueOf("2020-03-01")

  ),
  Row(

    "002", "u2", 1, 30.00,

    Date.valueOf("2020-04-01"), Date.valueOf("2020-04-01")

  )
)
val ordersDF = createDF(orders, schema)


// Create order_updates dataframe
val orderUpdates = Seq(
  Row(

    "002", "u2", 1, 20.00,

    Date.valueOf("2020-04-01"), Date.valueOf("2020-04-02")

  )
)
val orderUpdatesDF = createDF(orderUpdates, schema)


// Register temporary views
ordersDF.createOrReplaceTempView("orders")
orderUpdatesDF.createOrReplaceTempView("order_updates")

val orderReconciledDF = spark.sql(
  """
  |SELECT unioned.*
  |FROM (
  |  SELECT * FROM orders x
  |  UNION ALL
  |  SELECT * FROM order_updates y
  |) unioned
  |JOIN
  |(

  |  SELECT
  |    order_no,
  |    max(last_updated_date) as max_date
  |  FROM (
  |    SELECT * FROM orders
  |    UNION ALL
  |    SELECT * FROM order_updates
  |  ) t
  |  GROUP BY
  |    order_no
  |) grouped
  |ON
  |  unioned.order_no = grouped.order_no AND
  |  unioned.last_updated_date = grouped.max_date
  """.stripMargin
)


spark.sql(
  """
  |SELECT unioned.*
  |FROM (
  |  SELECT * FROM orders x
  |  UNION ALL
  |  SELECT * FROM order_updates y
  |) unioned
  |JOIN
  |(

  |  SELECT
  |    order_no,
  |    max(last_updated_date) as max_date
  |  FROM (
  |    SELECT * FROM orders
  |    UNION ALL
  |    SELECT * FROM order_updates
  |  ) t
  |  GROUP BY
  |    order_no
  |) grouped
  |ON
  |  unioned.order_no = grouped.order_no AND
  |  unioned.last_updated_date = grouped.max_date
  """.stripMargin
).show()