import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


public class StructuredStreamTest {
    public static void main(String[] args) throws StreamingQueryException {

        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);

        SparkSession spark = SparkSession
                .builder()
                .appName("Structured Stream example")
                .master("local[4]")
                .getOrCreate();

        //create schema for json message
        StructType schema =  DataTypes.createStructType(new StructField[] {

                DataTypes.createStructField("id", DataTypes.IntegerType, true),
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("time", DataTypes.TimestampType, true)

        });

        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "quickstart.cloudera:9092")
                .option("subscribe", "test1")
                .load()
                .selectExpr("CAST(value AS STRING) as message")
                .select(functions.from_json(functions.col("message"),schema).as("json"))
                .select("json.*");

        //df.selectExpr( "CAST(value AS STRING )");

        /*StructField[] structFields = new StructField[]{
                new StructField("id", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("name", DataTypes.StringType, true, Metadata.empty()),
                new StructField("time", DataTypes.LongType, true, Metadata.empty())

        };

         StructType structType = new StructType(structFields);*/

        //Dataset<Row> wordCounts=df.groupBy("id").max("time");

        /*Dataset<Row> windowedCounts = df.groupBy(
                functions.window(df.col("time"), "5 seconds", "5 seconds"),
                df.col("time")
        ).max("time");*/
        //.withWatermark("time", "5 seconds")
       /* Dataset<Row> windowedCounts = df
                 .groupBy(df.col("id"),
                functions.window(df.col("time"), "15 seconds")
        ).count();*/
        //Dataset<Row> windowedCounts2= windowedCounts.sort(windowedCounts.col("window"));
      // Dataset<Row> windowedCounts = df.withWatermark("time", "5 seconds").groupBy("time","id").count();

        Dataset<Row> windowedCounts = df.groupBy("time","id","name").count();

        StreamingQuery query=  windowedCounts
                .writeStream()
                .format("console")
                .outputMode("update")
                .option("truncate","false")
                .trigger(Trigger.ProcessingTime("15 seconds"))
                .start();

        query.awaitTermination();


    }


}
