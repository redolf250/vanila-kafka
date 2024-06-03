package com.redolf.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RewardPointAccumulator {
    private String customerId;
    private int currentRewardPoints;
    private int totalRewardPoints;
    private String lastPurchaseDate;
    private int daysFromLastPurchase;
}
