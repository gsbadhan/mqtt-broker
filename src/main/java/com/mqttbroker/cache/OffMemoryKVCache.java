package com.mqttbroker.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;


public class OffMemoryKVCache<K extends String, V extends Object> implements KVcache<K, V> {
    private final Logger log = LoggerFactory.getLogger(OffMemoryKVCache.class);
    private RedisTemplate<String, Object> redisTemplate;
    private String namespace;
    private long maxSize;
    private Duration ttl;


    public OffMemoryKVCache(RedisTemplate<String, Object> redisTemplate, String namespace, long maxSize, Duration ttl) {
        this.namespace = namespace;
        this.maxSize = maxSize;
        this.ttl = ttl;
        this.redisTemplate = redisTemplate;
        log.info("OffMemoryCache instance created namespace{}, maxSize={}, ttl={}", namespace, maxSize, ttl);
    }

    @Override
    public V put(K key, V value) {
        redisTemplate.opsForValue().set(key(namespace, key), ttl);
        return (V) redisTemplate.opsForValue().get(key(namespace, key));
    }

    @Override
    public V put(K key, V value, Duration ttl) {
        redisTemplate.opsForValue().set(key(namespace, key), ttl);
        return (V) redisTemplate.opsForValue().get(key(namespace, key));
    }

    @Override
    public V get(K key) {
        return (V) redisTemplate.opsForValue().get(key(namespace, key));
    }

    @Override
    public V delete(K key) {
        redisTemplate.delete(key(namespace, key));
        return null;
    }

    @Override
    public boolean contains(K key) {
        V value = (V) redisTemplate.opsForValue().get(key(namespace, key));
        if (value == null) {
            return false;
        }
        return true;
    }
}
