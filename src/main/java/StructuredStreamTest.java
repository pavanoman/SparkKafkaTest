import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
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
                DataTypes.createStructField("time", DataTypes.LongType, true)

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



        StreamingQuery query=  df.writeStream()
                .format("console")

                .option("truncate","false")
                .start();

        query.awaitTermination();


    }
}
