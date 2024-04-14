package com.redolf.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PurchasePattern {
    private String itemName;
    private String zipCode;
    private String purchasedAt;
}
