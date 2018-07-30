import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class SparkStreamingApp {


    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf().setAppName("SparkstreamExample")
                .setMaster("local[4]");
        //set("spark.driver.host","localhost")
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        Map<String, Object> kafkaParams = new HashMap<String,Object>();
        kafkaParams.put("bootstrap.servers", "quickstart.cloudera:9094");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "java-test-consumer");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("test5");


        // Start reading messages from Kafka and get DStream
        /*JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );*/



        // Read value of each message from Kafka and return it
       /* JavaDStream<String> lines = stream.map(new Function<ConsumerRecord<String,String>, String>() {
            @Override
            public String call(ConsumerRecord<String, String> kafkaRecord) throws Exception {
                return kafkaRecord.value();
            }
        });*/

        JavaInputDStream<ConsumerRecord<String, String>> lines= KafkaUtils.createDirectStream(
                jssc
                ,LocationStrategies.PreferConsistent()
                ,ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
        );




        //lines.count();
        lines.print();
        jssc.start();
        jssc.awaitTermination();

    }


}
