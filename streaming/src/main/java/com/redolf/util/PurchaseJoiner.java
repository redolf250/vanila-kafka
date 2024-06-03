package com.redolf.util;

import com.redolf.model.CorrelatedPurchase;
import com.redolf.model.Purchase;
import org.apache.kafka.streams.kstream.ValueJoiner;

import java.util.ArrayList;
import java.util.List;

public class PurchaseJoiner implements ValueJoiner<Purchase, Purchase, CorrelatedPurchase> {
    @Override
    public CorrelatedPurchase apply(Purchase purchase, Purchase purchase2) {

        List<Purchase> purchasedItems = new ArrayList<>();
        if (purchase != null) {
            purchasedItems.add(purchase);
        }
        if (purchase2 != null) {
            purchasedItems.add(purchase2);
        }

        assert purchase != null;
        assert purchase2 != null;
        if (purchase.getCustomerId().equalsIgnoreCase(purchase2.getCustomerId()))
            return CorrelatedPurchase.builder()
                    .customerId(purchase.getCustomerId())
                    .items(purchasedItems)
                    .correlatedTotalAmount(purchase.getTotalAmount()+purchase2.getTotalAmount())
                    .build();
        return null;
    }
}
