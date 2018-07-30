object ProducerExample extends App {
 
 import java.util.Properties

 import org.apache.kafka.clients.producer._

 import scala.io.Source

 val  props = new Properties()
 props.put("bootstrap.servers", "quickstart.cloudera:9094")
  
 props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
 props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

 val producer = new KafkaProducer[String,String](props)
   
 val TOPIC="test4"



val source = Source.fromFile("/home/cloudera/Documents/README.txt")

for (line <- source.getLines()){

  val record = new ProducerRecord[String, String ](TOPIC,"key" ,line)
  producer.send(record)

}
   
 source.close()
 producer.close()


}
