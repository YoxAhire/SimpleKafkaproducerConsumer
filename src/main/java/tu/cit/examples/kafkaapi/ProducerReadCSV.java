package tu.cit.examples.kafkaapi;


import org.apache.kafka.clients.producer.RecordMetadata;
import tu.cit.examples.kafkaapi.serde.JsonSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import tu.cit.examples.kafkaapi.schemas.student;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;


public class ProducerReadCSV {


    public static void main(String[] args) throws InterruptedException, ExecutionException {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"10.151.34.116:6667");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);




        KafkaProducer<String,student> producer = new KafkaProducer<String,student>(props);


        ReadCSV readCSV = new ReadCSV();
        List studentList = readCSV.ReadCSVFile(); //It will return the student list

        Long current_time = System.currentTimeMillis();

        for (Object studentObject : studentList) {
            student stdobject = (student) studentObject;

            producer.send(new ProducerRecord<String, student>("rr135",stdobject.getDept(),stdobject));

        }

        System.out.println("required time : "+(System.currentTimeMillis()-current_time));

        producer.close();
    }


}
