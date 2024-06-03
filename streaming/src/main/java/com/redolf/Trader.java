package com.redolf;

import com.redolf.model.StockTransaction;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

import static com.redolf.node.KTableProcessorNodes.parentStockNode;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;

public class Trader {
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put(APPLICATION_ID_CONFIG, "trade_application");
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put("auto.create.topics.enable",true);

        StreamsConfig streamingConfig = new StreamsConfig(props);
        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, StockTransaction> stringStockTransactionKTable = parentStockNode(builder);


        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(),streamingConfig);
        kafkaStreams.start();
        Thread.sleep(55000);
        System.out.println("Shutting down Kafka Streams");
        kafkaStreams.close();
    }
}
