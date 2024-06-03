package com.redolf.util;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

public class MyStoreBuilder {
    public static StoreBuilder<KeyValueStore<String, Integer>> build() {
        return Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("my-state-store"),
                        Serdes.String(),
                        Serdes.Integer()
                )
                .withLoggingDisabled() // If you don't want to log state store changes
                .withCachingEnabled(); // Enable caching for better performance
    }
}
