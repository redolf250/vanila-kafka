package com.redolf;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class Streaming {
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streaming_app_id");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        StreamsConfig streamingConfig = new StreamsConfig(props);
        Serde<String> stringSerde = Serdes.String();
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> simpleFirstStream = builder.stream("zmart-transactions", Consumed.with(stringSerde, stringSerde));

        KStream<String, String> upperCasedStream = simpleFirstStream.mapValues((key, value) -> value.toUpperCase());
        upperCasedStream.peek((key, value) -> System.out.println(value));

//        upperCasedStream.to( "out-topic", Produced.with(stringSerde, stringSerde));
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(),streamingConfig);
        kafkaStreams.start();
        Thread.sleep(35000);
        System.out.println("Shutting down Kafka Streams");
        kafkaStreams.close();
    }
}
