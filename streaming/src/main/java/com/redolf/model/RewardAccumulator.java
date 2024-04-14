package com.redolf.model;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RewardAccumulator {
    private String customerId;
    private String customerName;
    private String customerNumber;
    private double totalAmount;
    private int currentRewardPoints;
    private int daysFromLastPurchase;
    private long totalRewardPoints;
}
