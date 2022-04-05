package tu.cit.examples.kafkaapi;
import org.apache.kafka.clients.producer.*;
import tu.cit.examples.kafkaapi.serde.JsonSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import tu.cit.examples.kafkaapi.schemas.student;
import java.util.List;
import java.util.Properties;


public class ProducerReadCSVAsync {

    public static void main(String[] args) throws InterruptedException {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"10.151.34.116:6667");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);




        KafkaProducer<String,student> producer = new KafkaProducer<String,student>(props);


        ReadCSV readCSV = new ReadCSV();
        List studentList = readCSV.ReadCSVFile(); //It will return the student list

        for (Object studentObject : studentList) {
            student stdobject = (student) studentObject;
            Thread.sleep(1000);
            ProducerRecord<String, student> record    =  new ProducerRecord<String, student>("rr18",stdobject.getDept(),stdobject);

            try {
                producer.send(record, new MyCallBack(stdobject.toString()));
            }catch (Exception ex)
            {
                System.out.println("Producer failed with an exception "+ex);
            }

        }

        producer.close();
    }

}
class MyCallBack implements Callback{

    private String msg;

    MyCallBack(String msg){
      this.msg = msg;
    }

    public void onCompletion(RecordMetadata metadata, Exception e) {

        if(e != null)
        {
            System.out.println("Producer failed with an exception "+e);

        }else{

            //System.out.println("Producer call successfully sent the msg to Broker ");
            System.out.println(msg);
            System.out.println("Record return to Offset: "+metadata.offset());
            System.out.println("Record return to Partition: "+metadata.partition());
            System.out.println("Record return to Topic: "+metadata.topic());;
        }

    }
}