package com.redolf.node;

import com.redolf.builders.ModelBuilders;
import com.redolf.model.*;
import com.redolf.util.PurchaseJoiner;
import com.redolf.util.RewardTransformer;
import com.redolf.util.RewardsStreamPartitioner;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;

import static com.redolf.convertor.JsonConverters.*;

public class ProcessorNodes {

    public static KStream<String, Purchase> purchaseNode(StreamsBuilder builder) {
        KStream<String, Purchase> purchaseKStream = builder
                .stream("zmart-transactions", Consumed.with(Serdes.String(), purchaseSerde()))
                .mapValues(ModelBuilders::maskCreditCardNumber);
        final KStream<Integer, Purchase> purchaseKStreamWithKey = purchaseKStream.selectKey((s, purchase) -> purchase.getQuantity());
        purchaseKStreamWithKey.to("zmart-purchases", Produced.with(Serdes.Integer(), purchaseSerde()));
        purchaseKStreamWithKey.print(Printed.<Integer, Purchase>toSysOut()
                .withLabel("zmart-transactions"));
        return purchaseKStream;
    }

    public static KStream<String, PurchasePattern> purchasePatternNode(KStream<String, Purchase> purchaseKStream) {
        KStream<String, PurchasePattern> patternKStream = purchaseKStream.mapValues(ModelBuilders::purchasePattern);
        patternKStream.print(Printed.<String, PurchasePattern>toSysOut()
                .withLabel("zmart-patterns"));
        patternKStream.to("zmart-patterns", Produced.with(Serdes.String(), purchasePatternSerde()));
        return patternKStream;
    }

    public static KStream<String, RewardAccumulator> rewardNode(KStream<String, Purchase> purchaseKStream) {
        KStream<String, RewardAccumulator> rewardKStream = purchaseKStream.mapValues(ModelBuilders::rewardAccumulator);
        rewardKStream.print(Printed.<String, RewardAccumulator>toSysOut()
                .withLabel("zmart-rewards"));
        rewardKStream.to("zmart-rewards", Produced.with(Serdes.String(), rewardAccumulatorSerde()));
        return rewardKStream;
    }

    public static KStream<String, RewardPointAccumulator> rewardNodeWithAccumulatedPoint(KStream<String, Purchase> purchaseKStream, String rewardsStateStoreName) {

        RewardsStreamPartitioner partitioner = new RewardsStreamPartitioner();
        KStream<String, Purchase> transByCustomerStream = purchaseKStream
                .through("zmart-customer-transactions", Produced.with(Serdes.String(), purchaseSerde(), partitioner));
//                .withStreamPartitioner(new RewardsStreamPartitioner()));

        final KStream<String, RewardPointAccumulator> stringRewardAccumulatorKStream = transByCustomerStream
                .transformValues(RewardTransformer::new, rewardsStateStoreName);

        stringRewardAccumulatorKStream.print(Printed.<String, RewardPointAccumulator>toSysOut()
                .withLabel("zmart-rewards-points"));
        stringRewardAccumulatorKStream.to("zmart-rewards-points", Produced.with(Serdes.String(), rewardPointsAccumulatorSerde()));
        return stringRewardAccumulatorKStream;
    }

    public static KStream<String, Purchase>[] branchNode(KStream<String, Purchase> purchaseKStream) {
        int coffee = 0;
        int electronics = 1;
        final KStream<String, Purchase> stringPurchaseKStream = partitionWithKey(purchaseKStream);

        Predicate<String, Purchase> isCoffee = (key, purchase) -> purchase.getCategory().equalsIgnoreCase("coffee");
        Predicate<String, Purchase> isElectronics = (key, purchase) -> purchase.getCategory().equalsIgnoreCase("electronics");

        final KStream<String, Purchase>[] branch = stringPurchaseKStream.branch(isCoffee, isElectronics);
        branch[coffee].to("zmart-coffee", Produced.with(Serdes.String(), purchaseSerde()));
        branch[electronics].to("zmart-electronics", Produced.with(Serdes.String(), purchaseSerde()));
        branch[coffee].print(Printed.<String, Purchase>toSysOut().withLabel("zmart-coffee"));
        branch[electronics].print(Printed.<String, Purchase>toSysOut().withLabel("zmart-electronics"));

        return branch;
    }

    public static KStream<String, Purchase> partitionWithKey(KStream<String,Purchase> purchaseKStream){
        return purchaseKStream.selectKey((s, purchase) -> purchase.getCustomerId());
    }

    public static KStream<String, Purchase>[] branchProcessorWithKey(KStream<String,Purchase> purchaseKStream){
        final KStream<String, Purchase> stringPurchaseKStream = partitionWithKey(purchaseKStream);
        Predicate<String, Purchase> isCoffee = (key, purchase) -> purchase.getCategory().equalsIgnoreCase("coffee");
        Predicate<String, Purchase> isElectronics = (key, purchase) -> purchase.getCategory().equalsIgnoreCase("electronics");
        final KStream<String, Purchase>[] branch = stringPurchaseKStream.branch(isCoffee, isElectronics);
        branch[0].print(Printed.<String, Purchase>toSysOut().withLabel("coffee-branch-node-with-key"));
        branch[1].print(Printed.<String, Purchase>toSysOut().withLabel("electronics-branch-node-with-key"));
        return branch;
    }

    public static KStream<String, CorrelatedPurchase> branchPurchaseJoiner(KStream<String, Purchase> purchaseKStream) {

        KStream<String, Purchase> stringPurchaseKStream = purchaseKStream.selectKey((s, purchase) -> purchase.getCustomerId());
        Predicate<String, Purchase> isCoffee = (key, purchase) -> purchase.getCategory().equalsIgnoreCase("coffee");
        Predicate<String, Purchase> isElectronics = (key, purchase) -> purchase.getCategory().equalsIgnoreCase("electronics");

        final KStream<String, Purchase>[] branch = stringPurchaseKStream.branch(isCoffee, isElectronics);
        KStream<String, Purchase> coffeeStream = branch[0];
        KStream<String, Purchase> electronicsStream = branch[1];

        ValueJoiner<Purchase, Purchase, CorrelatedPurchase> purchaseJoiner = new PurchaseJoiner();
        final KStream<String, CorrelatedPurchase> correlatedPurchaseKStream =
                coffeeStream
                        .join(electronicsStream, purchaseJoiner, JoinWindows.of(Duration.ofMinutes(5)));
        correlatedPurchaseKStream.print(Printed.<String, CorrelatedPurchase>toSysOut().withLabel("zmart-correlated-purchase"));
        correlatedPurchaseKStream.to("zmart-correlated-purchase",Produced.with(Serdes.String(),correlatedPurchaseSerde()))
        ;

        return correlatedPurchaseKStream;
    }

}
