package com.mqttbroker.cache;

import java.time.Duration;

public interface KVcache<K, V> {
    V put(K key, V value);

    V put(K key, V value, Duration ttl);

    V get(K key);

    V delete(K key);

    boolean contains(K key);

    default String key(String namespace, K key) {
        return new StringBuilder(namespace).append(":").append(key).toString();
    }
}
