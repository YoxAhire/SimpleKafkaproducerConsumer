package tu.cit.examples.kafkaapi;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import tu.cit.examples.kafkaapi.schemas.student;
import tu.cit.examples.kafkaapi.serde.JsonDeserializer;

import java.util.Properties;

public class StreamApplication {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,"Stream1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"10.151.34.116:6667");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());


        //props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,Serdes.String().getClass());
        //props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonDeserializer.class);


        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> kStream = streamsBuilder.stream("rr135");
        //KStream<String, student> kStream = streamsBuilder.stream("rr135");

        kStream.foreach( (k,v)->System.out.println("key: "+k + "Value: "+ v));

        Topology topology = streamsBuilder.build();
        KafkaStreams streams = new KafkaStreams(topology,props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            streams.close();
        }));
    }
}
