import org.apache.log4j.{Level, Logger}

object StreamingTest extends App {

  import java.util.Properties

  import org.apache.kafka.clients.producer._

  import scala.io.Source

  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.SparkSession

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  val spark = SparkSession
    .builder
    .master("local[4]")
    .appName("StructuredCount")
    .getOrCreate()

  import spark.implicits._


  val lines = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()

  val words = lines.as[String].flatMap(_.split(" "))

  val wordCounts = words.groupBy("value").count()

  val query = wordCounts.writeStream
    .outputMode("complete")
    .format("console")
    .start()

  query.awaitTermination()

}
