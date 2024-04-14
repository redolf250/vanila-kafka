package com.redolf.node;

import com.redolf.builders.ModelBuilders;
import com.redolf.model.Purchase;
import com.redolf.model.PurchasePattern;
import com.redolf.model.RewardAccumulator;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import static com.redolf.convertor.JsonConverters.*;

public class ProcessorNodes {

    public static KStream<String, Purchase> purchaseNode( StreamsBuilder builder){
        KStream<String, Purchase> purchaseKStream = builder
                .stream("zmart-transactions", Consumed.with(Serdes.String(), purchaseSerde()))
                .mapValues(ModelBuilders::maskCreditCardNumber);
        final KStream<Integer, Purchase> purchaseKStreamWithKey = purchaseKStream.selectKey((s, purchase) -> purchase.getQuantity());
        purchaseKStreamWithKey.to("zmart-purchases", Produced.with(Serdes.Integer(), purchaseSerde()));
        purchaseKStreamWithKey.print(Printed.<Integer, Purchase>toSysOut()
                .withLabel("zmart-transactions"));
        return purchaseKStream;
    }

    public static KStream<String, PurchasePattern> purchasePatternNode(KStream<String, Purchase> purchaseKStream){
        KStream<String, PurchasePattern> patternKStream = purchaseKStream.mapValues(ModelBuilders::purchasePattern);
        patternKStream.print(Printed.<String, PurchasePattern>toSysOut()
                .withLabel("zmart-patterns"));
        patternKStream.to("zmart-patterns", Produced.with(Serdes.String(),purchasePatternSerde()));
        return patternKStream;
    }

    public static KStream<String, RewardAccumulator> rewardNode(KStream<String, Purchase> purchaseKStream){
        KStream<String, RewardAccumulator> rewardKStream = purchaseKStream.mapValues(ModelBuilders::rewardAccumulator);
        rewardKStream.print(Printed.<String, RewardAccumulator>toSysOut()
                .withLabel("zmart-rewards"));
        rewardKStream.to("zmart-rewards", Produced.with(Serdes.String(),rewardAccumulatorSerde()));
        return rewardKStream;
    }

    public static KStream<String, Purchase>[] branchNode(KStream<String, Purchase> purchaseKStream){
        int coffee = 0;
        int electronics = 1;
        Predicate<String, Purchase> isCoffee = (key, purchase) -> purchase.getCategory().equalsIgnoreCase("coffee");
        Predicate<String, Purchase> isElectronics = (key, purchase) -> purchase.getCategory().equalsIgnoreCase("electronics");
        final KStream<String, Purchase>[] branch = purchaseKStream.branch(isCoffee, isElectronics);
        branch[coffee].to( "zmart-coffee", Produced.with(Serdes.String(), purchaseSerde()));
        branch[electronics].to( "zmart-electronics", Produced.with(Serdes.String(), purchaseSerde()));
        branch[coffee].print(Printed.<String, Purchase>toSysOut().withLabel("zmart-coffee"));
        branch[electronics].print(Printed.<String, Purchase>toSysOut().withLabel("zmart-electronics"));
        return  branch;
    }


}
