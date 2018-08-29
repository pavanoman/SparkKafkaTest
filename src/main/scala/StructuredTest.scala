import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{Dataset, Row, functions}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object StructuredTest extends App {

  import org.apache.spark.sql.SparkSession

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  val spark = SparkSession
    .builder
    .master("local[4]")
    .appName("StructuredCount")
    .getOrCreate()

  import spark.implicits._


  val schema = DataTypes
    .createStructType(Array[StructField](
      DataTypes.createStructField("id", DataTypes.IntegerType, true),
      DataTypes.createStructField("name", DataTypes.StringType, true),
      DataTypes.createStructField("time", DataTypes.TimestampType, true)))

  val df2 = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "quickstart.cloudera:9092")
    .option("subscribe", "test1").load.selectExpr("CAST(value AS STRING) as message")
    .select(functions.from_json(functions.col("message"), schema).as("json"))
    .select("json.*")

  //val df2 = df.groupBy("time", "id", "name").count



  /*val df3= df2.join(
    df2.groupBy($"id").agg(org.apache.spark.sql.functions.max($"time") as "r_time").withColumnRenamed("id", "r_id"),
    $"id" === $"r_id" && $"time" === $"r_time"
  ).drop("r_id").drop("r_time")*/


  val df3=df2.groupBy($"id").agg( org.apache.spark.sql.functions.max($"time"))

  val query = df3
    .writeStream
    .format("console")
    .outputMode("update")
    .option("truncate", "false")
    .trigger(Trigger.ProcessingTime("15 seconds"))
    .start

  query.awaitTermination()



}
