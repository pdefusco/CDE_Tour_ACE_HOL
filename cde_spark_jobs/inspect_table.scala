import org.apache.iceberg.hive.HiveCatalog
import scala.collection.JavaConverters._
import org.apache.iceberg.catalog.TableIdentifier

val catalog = new HiveCatalog()

catalog.setConf(spark.sessionState.newHadoopConf())
catalog.initialize("hive", Map.empty[String, String].asJava)

val tId = TableIdentifier.of("spark_iceberg", "customer_partitioned")
val table = catalog.loadTable(tId)

table.spec
