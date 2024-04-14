package com.redolf.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Purchase {
    private String firstName;
    private String lastName;
    private String customerNumber;
    private String creditNumber;
    private String itemName;
    private String zipCode;
    private String category;
    private int quantity;
    private double unitPrice;
    private double totalAmount;
    private String purchasedAt;
}
