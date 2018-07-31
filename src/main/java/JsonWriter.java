import org.apache.commons.lang.RandomStringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;

public class JsonWriter {


    public static int count=1;
    public static String getJSONString() throws IOException {

        Random rand = new Random();
        //int id=rand.nextInt();
        int id=count;

        if(count%5==0 ) {

            id=rand.nextInt(count);
        }
        count++;

        int length = 10;
        boolean useLetters = true;
        boolean useNumbers = false;
        String name = RandomStringUtils.random(length, useLetters, useNumbers);

        long time=System.currentTimeMillis();

        User user = new User (id,name,time);
        ObjectMapper objectMapper= new ObjectMapper();
        String objString=objectMapper.writeValueAsString(user);

        return objString;
    }

    public static void main(String[] args) throws IOException {


        //pojo to json
        //json to kafka
        //retention period of 15 mins

        Properties props = new Properties();
        props.put("bootstrap.servers", "quickstart.cloudera:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer <String, String> producer = new KafkaProducer <String,String>(props);

        int count2=0;
        try {
            while (true) {
                producer.send(new ProducerRecord<String, String>("test1", "key", getJSONString()));

                Thread.sleep(1 * 2000);
                count2++;
                if(count2==50) break;
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        producer.close();


    }

}
