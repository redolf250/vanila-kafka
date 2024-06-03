package com.redolf.convertor;

import com.redolf.model.*;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class JsonConverters {
    public static Serde<Purchase> purchaseSerde(){
        JsonSerializer<Purchase> purchaseJsonSerializer = new JsonSerializer<>();
        JsonDeserializer<Purchase> purchaseJsonDeserializer = new JsonDeserializer<>(Purchase.class);
        return Serdes.serdeFrom(purchaseJsonSerializer, purchaseJsonDeserializer);
    }
    public static Serde<PurchasePattern> purchasePatternSerde(){
        JsonSerializer<PurchasePattern> patternSerializer = new JsonSerializer<>();
        JsonDeserializer<PurchasePattern> patternDeserializer = new JsonDeserializer<>(PurchasePattern.class);
        return Serdes.serdeFrom(patternSerializer, patternDeserializer);
    }
    public static Serde<RewardAccumulator> rewardAccumulatorSerde(){
        JsonSerializer<RewardAccumulator> patternSerializer = new JsonSerializer<>();
        JsonDeserializer<RewardAccumulator> patternDeserializer = new JsonDeserializer<>(RewardAccumulator.class);
        return Serdes.serdeFrom(patternSerializer, patternDeserializer);
    }

    public static Serde<RewardPointAccumulator> rewardPointsAccumulatorSerde(){
        JsonSerializer<RewardPointAccumulator> serializer = new JsonSerializer<>();
        JsonDeserializer<RewardPointAccumulator> deserializer = new JsonDeserializer<>(RewardPointAccumulator.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }

    public static Serde<CorrelatedPurchase> correlatedPurchaseSerde(){
        JsonSerializer<CorrelatedPurchase> serializer = new JsonSerializer<>();
        JsonDeserializer<CorrelatedPurchase> deserializer = new JsonDeserializer<>(CorrelatedPurchase.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }

    public static Serde<StockTransaction> stockTickerSerde(){
        JsonSerializer<StockTransaction> serializer = new JsonSerializer<>();
        JsonDeserializer<StockTransaction> deserializer = new JsonDeserializer<>(StockTransaction.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }
}
