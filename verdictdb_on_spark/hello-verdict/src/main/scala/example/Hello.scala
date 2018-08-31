package example

import org.apache.spark.sql.SparkSession
import scala.util.Random

object Hello extends App {
  val spark = SparkSession
    .builder()
    .appName("VerdictDB basic example")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  insertData(spark)
  val query = "select count(*) from myschema.sales"
  val df = spark.sql(query)
  df.show()

  def insertData(spark: SparkSession): Unit = {
    // create a schema and a table
    spark.sql("DROP SCHEMA IF EXISTS myschema CASCADE")
    spark.sql("CREATE SCHEMA IF NOT EXISTS myschema")
    spark.sql("CREATE TABLE IF NOT EXISTS myschema.sales (product string, price double)")

    // insert 1000 rows
    val productList = List("milk", "egg", "juice")
    val rand = new Random()
    var query = "INSERT INTO myschema.sales VALUES"
    for (i <- 0 until 1000) {
      val randInt: Int = rand.nextInt(3)
      val product: String = productList(randInt)
      val price: Double = (randInt+2) * 10 + rand.nextInt(10)
      if (i == 0) {
        query = query + f" ('$product', $price%.0f)"
      } else {
        query = query + f", ('$product', $price%.0f)"
      }
    }
    spark.sql(query)
  }
}