package com.redolf.builders;

import com.redolf.model.RewardAccumulator;
import com.redolf.model.Purchase;
import com.redolf.model.PurchasePattern;
import com.redolf.model.RewardPointAccumulator;
import org.joda.time.LocalDateTime;

import java.time.Duration;
import java.time.LocalDate;
import java.time.Period;
import java.util.OptionalInt;
import java.util.Random;
import java.util.stream.LongStream;

public class ModelBuilders {
    public static PurchasePattern purchasePattern(Purchase purchase){
        return PurchasePattern.builder()
                .itemName(purchase.getItemName())
                .zipCode(purchase.getZipCode())
                .purchasedAt(purchase.getPurchasedAt())
                .build();
    }
    public static RewardAccumulator rewardAccumulator(Purchase purchase){
        return RewardAccumulator.builder()
                .customerId(purchase.getCustomerId())
                .currentRewardPoints((int) purchase.getTotalAmount())
                .customerName(String.format("%s %s", purchase.getFirstName(),purchase.getLastName()))
                .customerNumber(purchase.getCustomerNumber())
                .totalAmount(purchase.getTotalAmount())
                .build();
    }

    public static  RewardPointAccumulator rewardAccumulatorUsingStateStore(Purchase purchase){
        return  RewardPointAccumulator.builder()
                .customerId(purchase.getCustomerId())
                .currentRewardPoints((int) purchase.getTotalAmount())
                .lastPurchaseDate(purchase.getPurchasedAt())
                .daysFromLastPurchase(calculateDaysElapsed(purchase))
                .build();
    }

    public static Purchase maskCreditCardNumber(Purchase purchase){
        return Purchase.builder()
                .customerId(purchase.getCustomerId())
                .firstName(purchase.getFirstName())
                .lastName(purchase.getLastName())
                .customerNumber(purchase.getCustomerNumber())
                .creditNumber(creditMaskers(purchase.getCreditNumber()))
                .itemName(purchase.getItemName())
                .zipCode(purchase.getZipCode())
                .category(purchase.getCategory())
                .quantity(purchase.getQuantity())
                .unitPrice(purchase.getUnitPrice())
                .totalAmount(purchase.getTotalAmount())
                .purchasedAt(purchase.getPurchasedAt())
                .build();
    }
    public static String creditMaskers(String cardNumber){
        final String[] split = new String[]{cardNumber.split("-")[3]};
        return String.format("xxxx-xxxx-xxxx-%s", split);
    }
    public static int calculateDaysElapsed(Purchase purchase){
        final String[] s = purchase.getPurchasedAt().split(" ");
        final String s1 = s[0];
        final LocalDate parse = LocalDate.parse(s1).minusDays(purchase.getQuantity());
        return Period.between(parse, LocalDate.now()).getDays();
    }

}
