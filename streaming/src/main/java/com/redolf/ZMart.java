package com.redolf;

import com.redolf.model.Purchase;
import com.redolf.model.PurchasePattern;
import com.redolf.model.RewardAccumulator;
import com.redolf.node.ProcessorNodes;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;

@Slf4j
public class ZMart {
    public static void main(String[] args) throws InterruptedException {

        Properties props = new Properties();
        props.put(APPLICATION_ID_CONFIG, "zmart_application");
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        StreamsConfig streamingConfig = new StreamsConfig(props);
        StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, Purchase> purchaseKStream = ProcessorNodes.purchaseNode(builder);

        final KStream<String, PurchasePattern> patternKStream = ProcessorNodes.purchasePatternNode(purchaseKStream);

        final KStream<String, RewardAccumulator> rewardNode = ProcessorNodes.rewardNode(purchaseKStream);

        final KStream<String, Purchase>[] branchNode = ProcessorNodes.branchNode(purchaseKStream);


        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(),streamingConfig);
        kafkaStreams.start();
//        Runtime.getRuntime(kafkaStreams.close());
        Thread.sleep(55000);
        System.out.println("Shutting down Kafka Streams");
        kafkaStreams.close();
    }
}
