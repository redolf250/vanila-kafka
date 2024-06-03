package com.redolf.util;

import com.redolf.model.Purchase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.concurrent.TimeUnit;

public class CustomTimestampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long l) {
        Purchase purchaseTransaction = (Purchase) consumerRecord.value();
        String stamp = purchaseTransaction.getPurchasedAt();
        String[] s = stamp.split(" ");
        String s0 = s[0];
        String s1 = s[1];
        LocalDateTime b = LocalDateTime.parse(s0+"T"+s1);
        return 0;
    }
}
