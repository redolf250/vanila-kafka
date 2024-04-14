package com.redolf.builders;

import com.redolf.model.RewardAccumulator;
import com.redolf.model.Purchase;
import com.redolf.model.PurchasePattern;

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
                .customerName(String.format("%s %s", purchase.getFirstName(),purchase.getLastName()))
                .customerNumber(purchase.getCustomerNumber())
                .totalAmount(purchase.getTotalAmount())
                .build();
    }
    public static Purchase maskCreditCardNumber(Purchase purchase){
        return Purchase.builder()
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

}
