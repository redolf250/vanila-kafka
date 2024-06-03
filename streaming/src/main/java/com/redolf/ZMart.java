package com.redolf;

import com.redolf.model.*;
import com.redolf.node.ProcessorNodes;
import com.redolf.util.MyStoreBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.redolf.node.ProcessorNodes.*;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;


public class ZMart {
    public static void main(String[] args) throws InterruptedException {

        Properties props = new Properties();
        props.put(APPLICATION_ID_CONFIG, "zmart_application");
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put("auto.create.topics.enable",true);

        StreamsConfig streamingConfig = new StreamsConfig(props);
        StreamsBuilder builder = new StreamsBuilder();

        Map<String, String> changeLogConfigs = new HashMap<>();
        changeLogConfigs.put("retention.ms","172800000" );
        changeLogConfigs.put("retention.bytes", "10000000000");
        changeLogConfigs.put("cleanup.policy", "compact,delete");

        String rewardsStateStoreName = "rewardsPointsStore";

        KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore(rewardsStateStoreName);
        StoreBuilder<KeyValueStore<String, Integer>> storeBuilder = Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), Serdes.Integer())
//                .withLoggingEnabled(changeLogConfigs)
                .withCachingEnabled();
        builder.addStateStore(storeBuilder);

        final KStream<String, Purchase> purchaseKStream = ProcessorNodes.purchaseNode(builder);

        final KStream<String, PurchasePattern> patternKStream = ProcessorNodes.purchasePatternNode(purchaseKStream);

        final KStream<String, RewardAccumulator> rewardNode = ProcessorNodes.rewardNode(purchaseKStream);

        final KStream<String, Purchase>[] branchNode = ProcessorNodes.branchNode(purchaseKStream);

        final KStream<String, RewardPointAccumulator> nodeWithAccumulatedPoint = rewardNodeWithAccumulatedPoint(purchaseKStream, rewardsStateStoreName);

        final KStream<String, Purchase>[] branchProcessorWithKey = branchProcessorWithKey(purchaseKStream);

//        final KStream<String, CorrelatedPurchase> correlatedPurchaseKStream = branchPurchaseJoiner(purchaseKStream);


        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(),streamingConfig);
//        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
        kafkaStreams.start();
//        Runtime.getRuntime(kafkaStreams.close());
        Thread.sleep(55000);
        System.out.println("Shutting down Kafka Streams");
        kafkaStreams.close();
    }
}
