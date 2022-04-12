package tu.cit.examples.kafkaapi;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import tu.cit.examples.kafkaapi.schemas.student;
import tu.cit.examples.kafkaapi.serde.JsonDeserializer;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class MyConsumer {


    public static void main(String[] args) throws InterruptedException, ExecutionException {

        System.out.println("In main method");
        Properties ConfigProps = new Properties();

        ConfigProps.put(ConsumerConfig.CLIENT_ID_CONFIG,"ConfID2");

        ConfigProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.151.34.116:6667");
        ConfigProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        ConfigProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

        ConfigProps.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG,student.class);

        ConfigProps.put(ConsumerConfig.GROUP_ID_CONFIG,"nice123");

        ConfigProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        KafkaConsumer<String, student> consumer = new KafkaConsumer<String, student>(ConfigProps);
        consumer.subscribe(Arrays.asList("rr135"));

        while (true){
            System.out.println("response:-");
            ConsumerRecords<String,student> records = consumer.poll(1000);

            for (ConsumerRecord<String,student> record : records ){

                System.out.println("Key : "+record.key()
                               +", studentid : "+record.value().studentid
                               +", name : "+record.value().name
                               +", department : "+record.value().getDept()
                               +", subject : "+record.value().getSubject()
                               +", marks : "+record.value().getMarks()
                               +", result : "+record.value().getMarks());
            }


        }

    }

}