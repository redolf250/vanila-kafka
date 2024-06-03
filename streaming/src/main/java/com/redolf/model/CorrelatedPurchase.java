package com.redolf.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CorrelatedPurchase {
    private String customerId;
    private List<Purchase> items;
    private double correlatedTotalAmount;
}
