package com.mqttbroker.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;


public class InMemoryKVCache<K extends String, V> implements KVcache<K, V> {
    private final Logger log = LoggerFactory.getLogger(InMemoryKVCache.class);
    private Cache<String, V> inMemoryCache;
    private String namespace;
    private long maxSize;
    private Duration ttl;

    public InMemoryKVCache(String namespace, long maxSize, Duration ttl) {
        this.namespace = namespace;
        this.maxSize = maxSize;
        this.ttl = ttl;
        this.inMemoryCache =
                Caffeine.newBuilder().maximumSize(this.maxSize).expireAfterWrite(this.ttl).build();
        log.info("InMemoryCache instance created namespace{}, maxSize={}, ttl={}", namespace, maxSize, ttl);
    }

    @Override
    public V put(K key, V value) {
        this.inMemoryCache.put(key(namespace, key), value);
        return this.inMemoryCache.getIfPresent(key);
    }

    @Override
    public V put(K key, V value, Duration ttl) {
        this.inMemoryCache.put(key(namespace, key), value);
        return this.inMemoryCache.getIfPresent(key);
    }

    @Override
    public V get(K key) {
        return this.inMemoryCache.getIfPresent(key(namespace, key));
    }

    @Override
    public V delete(K key) {
        this.inMemoryCache.invalidate(key(namespace, key));
        return null;
    }

    @Override
    public boolean contains(K key) {
        V value = this.inMemoryCache.getIfPresent(key(namespace, key));
        if (value == null) {
            return false;
        }
        return true;
    }

}
