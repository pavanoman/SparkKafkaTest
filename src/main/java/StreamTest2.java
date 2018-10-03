import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/*class HiveSingleton{

    private static HiveContext hive_instance=null;

    public static HiveContext getInstance(SparkContext sc){
        if(hive_instance==null){
            hive_instance=new HiveContext(sc);
        }

        return hive_instance;
    }

}*/


public class StreamTest2 {

    private static final Duration WINDOW_LENGTH = Durations.seconds(45);
    private static final Duration SLIDE_INTERVAL=Durations.seconds(15);


    public static void main(String[] args) throws InterruptedException {

       Logger.getLogger("org").setLevel(Level.WARN);
       Logger.getLogger("akka").setLevel(Level.WARN);
        //String warehouseLocation = new File("spark-warehouse").getAbsolutePath();

        SparkSession spark = SparkSession
                .builder()
//                .master("yarn-client")
                .appName("Java Spark example")
                //.config("spark.sql.warehouse.dir", "hdfs:///user/hive/warehouse" )
                .enableHiveSupport()

                .getOrCreate();

        spark.sparkContext().getConf().set("spark.streaming.stopGracefullyOnShutdown","true");

        //SparkContext sc = spark.sparkContext();

      /*  System.out.println("******Spark Conf Properties ******");
        for (Tuple2<String, String> stringStringTuple2 : sc.getConf().getAll()) {
            System.out.println(stringStringTuple2._1+"="+stringStringTuple2._2);
        }
*/

//  /home/clairvoyant/pavan/spark-warehouse/
        spark.sql("show schemas").show();
        spark.sql("show tables").show();

        JavaSparkContext ctx = new JavaSparkContext(spark.sparkContext());
        JavaStreamingContext jssc = new JavaStreamingContext(ctx, SLIDE_INTERVAL);

        //HiveContext hiveContext = new HiveContext(spark.sparkContext());

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "hadoop33.clairvoyant.local:9092");
        //kafkaParams.put("bootstrap.servers", "quickstart.cloudera:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "group2");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("test9");

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );

        JavaDStream<String> lines = stream.map(ConsumerRecord::value);


        JavaDStream<String> windowDStream =
               lines.window(WINDOW_LENGTH, SLIDE_INTERVAL);

        StructField[] structFields = new StructField[]{
               new StructField("id", DataTypes.IntegerType, true, Metadata.empty()),
               new StructField("name", DataTypes.StringType, true, Metadata.empty()),
               new StructField("time", DataTypes.LongType, true, Metadata.empty())

        };

       StructType structType = new StructType(structFields);

       windowDStream.foreachRDD((rdd) -> {
           Dataset<Row> df = spark.read().schema(structType).json(rdd);
           df.show(false);
           System.out.println("****the count is:" + df.count());
           df.createOrReplaceTempView("tempt");

           Dataset<Row> df2= spark.sql("select * from tempt");
           Dataset<Row> df3=spark.sql("select id, MAX(time) as maxtime from tempt group by id");

           df2.createOrReplaceTempView("records1");
           df3.createOrReplaceTempView("records2");

           Dataset<Row> df4=spark.sql("SELECT r1.id,r1.name, r2.maxtime FROM records1 r1 JOIN records2 r2 ON r1.id = r2.id and r1.time=r2.maxtime");
           df4.show();


           //df4.createOrReplaceTempView("records3");
           //spark.sql("create table if not exists default.testtable");
           //as select * from records3
           df4.write().mode(SaveMode.Overwrite).insertInto("default.testtable");
           System.out.println("*****Data Written in Hive Table*****");

       });

        jssc.start();
        jssc.awaitTermination();


    }
}