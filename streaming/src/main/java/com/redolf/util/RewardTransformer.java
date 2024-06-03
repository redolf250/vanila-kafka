package com.redolf.util;

import com.redolf.model.Purchase;
import com.redolf.model.RewardAccumulator;
import com.redolf.model.RewardPointAccumulator;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.LocalDate;
import java.time.Period;
import java.util.Random;

import static com.redolf.builders.ModelBuilders.calculateDaysElapsed;
import static com.redolf.builders.ModelBuilders.purchasePattern;

public class RewardTransformer implements ValueTransformer<Purchase, RewardPointAccumulator> {
    private KeyValueStore<String, Integer> stateStore;

    @Override
    public void init(ProcessorContext context) {
        this.stateStore = context.getStateStore("rewardsPointsStore");
    }

    @Override
    public RewardPointAccumulator transform(Purchase purchase) {
        RewardPointAccumulator rewardAccumulator = RewardPointAccumulator.builder()
                .customerId(purchase.getCustomerId())
                .currentRewardPoints((int) purchase.getTotalAmount())
                .lastPurchaseDate(purchase.getPurchasedAt())
                .daysFromLastPurchase(calculateDaysElapsed(purchase))
                .build();
        Integer accumulatedSoFar = stateStore.get(purchase.getCustomerId());
        if (accumulatedSoFar != null) {
            rewardAccumulator.setTotalRewardPoints((int) (accumulatedSoFar + purchase.getTotalAmount()));
        }
        stateStore.put(rewardAccumulator.getCustomerId(),rewardAccumulator.getTotalRewardPoints());
        return rewardAccumulator;
    }

    @Override
    public void close() {
    }
}
